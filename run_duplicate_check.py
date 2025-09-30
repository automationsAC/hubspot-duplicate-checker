#!/usr/bin/env python3
"""
HubSpot Duplicate Check - Render Deployment Version
Simplified version for Render cron job deployment
"""

import os
import sys
import requests
import csv
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    print("⚠️  rapidfuzz not available, installing...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'rapidfuzz'])
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True

class HubSpotDuplicateChecker:
    def __init__(self):
        # Environment variables
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_api_key = os.environ.get('SUPABASE_API_KEY')
        self.hubspot_token = os.environ.get('HUBSPOT_TOKEN')
        self.airtable_token = os.environ.get('AIRTABLE_TOKEN')
        
        if not all([self.supabase_url, self.supabase_api_key, self.hubspot_token]):
            raise ValueError("Missing required environment variables: SUPABASE_URL, SUPABASE_API_KEY, HUBSPOT_TOKEN")
        
        self.supabase_headers = {
            "apikey": self.supabase_api_key,
            "Authorization": f"Bearer {self.supabase_api_key}",
            "Content-Type": "application/json"
        }
        
        self.hubspot_headers = {
            'Authorization': f'Bearer {self.hubspot_token}',
            'Content-Type': 'application/json'
        }
        
        # Configuration
        self.batch_size = 500
        self.max_batches = 2
        self.log_every = 100
        
        # Rate limiting tracking
        self.search_api_calls = []
        self.search_api_limit = 3  # Conservative: 3 requests per second (instead of 5)
        
        # Thread-safe rate limiting
        self.crm_api_lock = threading.Lock()
        self.search_api_lock = threading.Lock()
        self.crm_api_calls = []
        self.crm_api_limit = 80  # Conservative: 80 requests per 10 seconds (instead of 100)
        
        # Caching
        self.contact_cache = {}
        self.deal_cache = {}
        self.aloha_cache = {}
        self.cache_lock = threading.Lock()
        
        # Parallel processing configuration
        self.max_workers = 3  # Reduced to avoid rate limits
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def wait_for_crm_api_rate_limit(self):
        """Ensure we don't exceed 100 requests per 10 seconds for CRM API"""
        with self.crm_api_lock:
            current_time = time.time()
            
            # Remove calls older than 10 seconds
            self.crm_api_calls = [call_time for call_time in self.crm_api_calls if current_time - call_time < 10.0]
            
            # If we've made 100 calls in the last 10 seconds, wait
            if len(self.crm_api_calls) >= self.crm_api_limit:
                wait_time = 10.0 - (current_time - self.crm_api_calls[0])
                if wait_time > 0:
                    time.sleep(wait_time)
                    current_time = time.time()
            
            # Record this call
            self.crm_api_calls.append(current_time)

    def wait_for_search_api_rate_limit(self):
        """Ensure we don't exceed 5 requests/second for Search API"""
        with self.search_api_lock:
            current_time = time.time()
            
            # Remove calls older than 1 second
            self.search_api_calls = [call_time for call_time in self.search_api_calls if current_time - call_time < 1.0]
            
            # If we've made 5 calls in the last second, wait
            if len(self.search_api_calls) >= self.search_api_limit:
                wait_time = 1.0 - (current_time - self.search_api_calls[0])
                if wait_time > 0:
                    time.sleep(wait_time)
                    current_time = time.time()
            
            # Record this call
            self.search_api_calls.append(current_time)

    def get_unprocessed_leads_count(self) -> int:
        """Get total count of unprocessed leads"""
        try:
            url = f"{self.supabase_url}/rest/v1/contacts_grid_view"
            params = {
                "select": "id",
                "email": "not.is.null",
                "property_name": "not.is.null",
                "duplicate_check_completed_at": "is.null",
                "duplicate_check_fetched_at": "is.null",
                "limit": "100000"  # Get a large number to count
            }
            
            response = requests.get(url, headers=self.supabase_headers, params=params)
            response.raise_for_status()
            
            leads = response.json()
            return len(leads)
            
        except Exception as e:
            self.logger.error(f"❌ Error getting unprocessed count: {e}")
            return 0

    def get_unprocessed_leads(self, batch_size: int = 500, offset: int = 0) -> List[Dict]:
        """Get unprocessed leads from Supabase"""
        self.logger.info(f"🔍 Fetching batch: size={batch_size}, offset={offset}")
        
        try:
            url = f"{self.supabase_url}/rest/v1/contacts_grid_view"
            params = {
                "select": "id,email,first_name,last_name,property_name,country,phone,booking_url",
                "email": "not.is.null",
                "property_name": "not.is.null",
                "duplicate_check_completed_at": "is.null",
                "duplicate_check_fetched_at": "is.null",
                "limit": str(batch_size)
            }
            
            if offset > 0:
                params["offset"] = str(offset)
                self.logger.info(f"⚠️ Using offset {offset}")
            
            response = requests.get(url, headers=self.supabase_headers, params=params)
            response.raise_for_status()
            
            leads = response.json()
            self.logger.info(f"✅ Retrieved {len(leads)} leads")
            
            # Mark as fetched
            if leads:
                self.mark_leads_as_fetched([lead['id'] for lead in leads])
            
            return leads
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching leads: {e}")
            return []

    def mark_leads_as_fetched(self, lead_ids: List[int]) -> bool:
        """Mark leads as fetched for duplicate checking"""
        if not lead_ids:
            return True
            
        self.logger.info(f"📝 Marking {len(lead_ids)} leads as fetched...")
        
        try:
            url = f"{self.supabase_url}/rest/v1/contacts_grid_view"
            current_time = datetime.now().isoformat()
            
            # Update in batches
            batch_size = 100
            for i in range(0, len(lead_ids), batch_size):
                batch_ids = lead_ids[i:i + batch_size]
                id_filter = ",".join(map(str, batch_ids))
                
                params = {"id": f"in.({id_filter})"}
                payload = {"duplicate_check_fetched_at": current_time}
                
                response = requests.patch(url, headers=self.supabase_headers, params=params, json=payload)
                response.raise_for_status()
                
                self.logger.info(f"✅ Marked batch {i//batch_size + 1} as fetched ({len(batch_ids)} leads)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error marking leads as fetched: {e}")
            return False

    def unmark_leads_as_fetched(self, lead_ids: List[int]) -> bool:
        """Unmark leads as fetched so they can be processed again"""
        if not lead_ids:
            return True
            
        self.logger.info(f"🔄 Unmarking {len(lead_ids)} leads as fetched (for retry)...")
        
        try:
            url = f"{self.supabase_url}/rest/v1/contacts_grid_view"
            
            # Update in batches
            batch_size = 100
            for i in range(0, len(lead_ids), batch_size):
                batch_ids = lead_ids[i:i + batch_size]
                id_filter = ",".join(map(str, batch_ids))
                
                params = {"id": f"in.({id_filter})"}
                payload = {"duplicate_check_fetched_at": None}  # Set to NULL
                
                response = requests.patch(url, headers=self.supabase_headers, params=params, json=payload)
                response.raise_for_status()
                
                self.logger.info(f"✅ Unmarked batch {i//batch_size + 1} as fetched ({len(batch_ids)} leads)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error unmarking leads as fetched: {e}")
            return False

    def search_hubspot_contact(self, lead: Dict) -> Tuple[Optional[str], Dict]:
        """Search for contact in HubSpot by email or phone"""
        email = lead.get('email', '').strip().lower()
        phone = self.normalize_phone(lead.get('phone', ''))
        
        # Try email first
        if email:
            cache_key = f"contact_email_{email}"
            with self.cache_lock:
                if cache_key in self.contact_cache:
                    return self.contact_cache[cache_key]
            
            try:
                # Apply CRM API rate limiting
                self.wait_for_crm_api_rate_limit()
                
                url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
                payload = {
                    "filterGroups": [{
                        "filters": [{
                            "propertyName": "email",
                            "operator": "EQ",
                            "value": email
                        }]
                    }],
                    "properties": ["email", "firstname", "lastname", "phone", "mobilephone"]
                }
                
                response = requests.post(url, headers=self.hubspot_headers, json=payload)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('results'):
                        contact = data['results'][0]
                        result = ('email_exact', {
                            'contact_id': contact['id'],
                            'contact_name': f"{contact['properties'].get('firstname', '')} {contact['properties'].get('lastname', '')}".strip(),
                            'contact_email_hs': contact['properties'].get('email', ''),
                            'contact_phone_hs': contact['properties'].get('phone', '') or contact['properties'].get('mobilephone', '')
                        })
                        with self.cache_lock:
                            self.contact_cache[cache_key] = result
                        return result
                
            except Exception as e:
                self.logger.warning(f"Error searching contact by email: {e}")
        
        # Try phone if email didn't work
        if phone:
            cache_key = f"contact_phone_{phone}"
            with self.cache_lock:
                if cache_key in self.contact_cache:
                    return self.contact_cache[cache_key]
            
            try:
                # Apply CRM API rate limiting
                self.wait_for_crm_api_rate_limit()
                
                url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
                payload = {
                    "filterGroups": [
                        {
                            "filters": [{
                                "propertyName": "phone",
                                "operator": "EQ", 
                                "value": phone
                            }]
                        },
                        {
                            "filters": [{
                                "propertyName": "mobilephone",
                                "operator": "EQ",
                                "value": phone
                            }]
                        }
                    ],
                    "properties": ["email", "firstname", "lastname", "phone", "mobilephone"]
                }
                
                response = requests.post(url, headers=self.hubspot_headers, json=payload)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('results'):
                        contact = data['results'][0]
                        result = ('phone_exact', {
                            'contact_id': contact['id'],
                            'contact_name': f"{contact['properties'].get('firstname', '')} {contact['properties'].get('lastname', '')}".strip(),
                            'contact_email_hs': contact['properties'].get('email', ''),
                            'contact_phone_hs': contact['properties'].get('phone', '') or contact['properties'].get('mobilephone', '')
                        })
                        with self.cache_lock:
                            self.contact_cache[cache_key] = result
                        return result
                
            except Exception as e:
                self.logger.warning(f"Error searching contact by phone: {e}")
        
        return ('none', {
            'contact_id': '',
            'contact_name': '',
            'contact_email_hs': '',
            'contact_phone_hs': ''
        })

    def normalize_phone(self, phone: str) -> str:
        """Normalize phone to E.164 format"""
        if not phone:
            return ''
        
        # Remove all non-digit characters except +
        cleaned = re.sub(r'[^\d+]', '', str(phone))
        
        # Add + if missing and looks international
        if cleaned and not cleaned.startswith('+') and len(cleaned) > 10:
            cleaned = '+' + cleaned
        
        return cleaned

    def search_hubspot_deals(self, lead: Dict) -> Tuple[bool, Dict]:
        """Search for deals in HubSpot using fuzzy matching"""
        property_name = lead.get('property_name', '').strip()
        if not property_name:
            return False, {}
        
        # Normalize property name for search
        normalized_property = self.normalize_text(property_name)
        search_terms = normalized_property.split()[:3]  # Use top 3 words
        
        cache_key = f"deal_{normalized_property}"
        with self.cache_lock:
            if cache_key in self.deal_cache:
                return self.deal_cache[cache_key]
        
        try:
            # Respect Search API rate limit
            self.wait_for_search_api_rate_limit()
            
            url = "https://api.hubapi.com/crm/v3/objects/deals/search"
            
            # Use free-text search
            search_query = ' '.join(search_terms)
            payload = {
                "query": search_query,
                "limit": 20,
                "properties": ["dealname", "dealstage", "country", "city", "address"]
            }
            
            response = requests.post(url, headers=self.hubspot_headers, json=payload)
            
            if response.status_code == 429:
                self.logger.warning(f"Rate limited (429), waiting 15 seconds...")
                time.sleep(15)
                response = requests.post(url, headers=self.hubspot_headers, json=payload)
                if response.status_code == 429:
                    self.logger.warning(f"Still rate limited after retry, waiting 30 seconds...")
                    time.sleep(30)
                    return False, {}
            
            if response.status_code != 200:
                self.logger.warning(f"Deal search failed: {response.status_code}")
                time.sleep(2)
                return False, {}
            
            data = response.json()
            best_match = None
            best_score = 0
            
            for deal in data.get('results', []):
                deal_name = deal['properties'].get('dealname', '')
                if not deal_name:
                    continue
                
                # Calculate fuzzy scores
                token_set_score = fuzz.token_set_ratio(normalized_property, self.normalize_text(deal_name))
                partial_token_score = fuzz.partial_token_sort_ratio(normalized_property, self.normalize_text(deal_name))
                score = max(token_set_score, partial_token_score)
                
                # Check location match
                location_match, location_details = self.check_location_match(lead, deal)
                
                # Scoring logic
                is_strong = score >= 92
                is_medium = 85 <= score < 92
                is_location_ok = location_match
                
                accept_match = False
                if is_strong and is_location_ok:
                    accept_match = True
                elif is_medium and is_location_ok:
                    accept_match = True
                elif is_strong and score >= 90:
                    accept_match = True
                
                if accept_match and score > best_score:
                    best_score = score
                    best_match = {
                        'deal_id': deal['id'],
                        'dealname': deal_name,
                        'deal_score': score,
                        'location_match': location_match,
                        'location_details': location_details,
                        'dealstage': deal['properties'].get('dealstage', '')
                    }
            
            result = (best_match is not None, best_match or {})
            with self.cache_lock:
                self.deal_cache[cache_key] = result
            return result
            
        except Exception as e:
            self.logger.warning(f"Error searching deals: {e}")
            return False, {}

    def normalize_text(self, text: str) -> str:
        """Normalize text for comparison"""
        if not text:
            return ''
        
        import unicodedata
        text = unicodedata.normalize('NFD', text)
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        
        # Convert to lowercase and remove extra spaces
        text = re.sub(r'\s+', ' ', text.lower().strip())
        
        # Remove common stop words for property names
        stop_words = ['hotel', 'pension', 'ferienwohnung', 'ferienhaus', 'apartment', 'villa', 'resort']
        words = text.split()
        words = [w for w in words if w not in stop_words]
        
        return ' '.join(words)

    def check_location_match(self, lead: Dict, deal: Dict) -> Tuple[bool, str]:
        """Check if location matches between lead and deal"""
        lead_country = (lead.get('country', '') or '').strip().lower()
        lead_city = (lead.get('city', '') or '').strip().lower()
        
        deal_country = (deal['properties'].get('country', '') or '').strip().lower()
        deal_city = (deal['properties'].get('city', '') or '').strip().lower()
        deal_address = (deal['properties'].get('address', '') or '').strip().lower()
        
        # Country matching
        country_match = False
        if lead_country and deal_country:
            country_codes = {
                'pl': 'poland', 'de': 'germany', 'es': 'spain',
                'poland': 'pl', 'germany': 'de', 'spain': 'es'
            }
            lead_country_norm = country_codes.get(lead_country, lead_country)
            deal_country_norm = country_codes.get(deal_country, deal_country)
            country_match = lead_country_norm == deal_country_norm
        
        # City matching
        city_match = False
        if lead_city and deal_city:
            city_match = fuzz.ratio(lead_city, deal_city) >= 90
        elif lead_city and deal_address:
            city_match = lead_city in deal_address
        
        # Overall location match
        if lead_city and deal_city:
            location_match = country_match and city_match
            details = f"country:{country_match}, city:{city_match}"
        else:
            location_match = country_match
            details = f"country:{country_match}"
        
        return location_match, details

    def check_alohacamp_existence(self, lead: Dict) -> Tuple[bool, Dict]:
        """Check if property exists in AlohaCamp (Airtable)"""
        if not self.airtable_token:
            return False, {}
        
        property_name = lead.get('property_name', '').strip()
        if not property_name:
            return False, {}
        
        cache_key = f"aloha_{self.normalize_text(property_name)}"
        with self.cache_lock:
            if cache_key in self.aloha_cache:
                return self.aloha_cache[cache_key]
        
        try:
            aloha_base = "appjLxzpDaVbvKGc1"
            aloha_table = "tblrfGtVp21mUgtlB"
            
            url = f"https://api.airtable.com/v0/{aloha_base}/{aloha_table}"
            headers = {
                'Authorization': f'Bearer {self.airtable_token}',
                'Content-Type': 'application/json'
            }
            
            params = {'maxRecords': 100}
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code != 200:
                return False, {}
            
            data = response.json()
            best_match = None
            best_score = 0
            
            normalized_property = self.normalize_text(property_name)
            
            for record in data.get('records', []):
                fields = record.get('fields', {})
                
                aloha_property_name = fields.get('Property Name', '')
                aloha_country = fields.get('Property Country', '')
                aloha_email = fields.get('Host Email (from Host)', [''])[0] if fields.get('Host Email (from Host)') else ''
                aloha_province = fields.get('Province', '')
                
                if not aloha_property_name:
                    continue
                
                score = fuzz.token_set_ratio(normalized_property, self.normalize_text(aloha_property_name))
                
                if score >= 90:
                    # Check location if available
                    location_ok = True
                    if lead.get('country') and aloha_country:
                        lead_country = lead['country'].lower()
                        aloha_country_norm = aloha_country.lower()
                        location_ok = lead_country == aloha_country_norm
                    
                    if location_ok and score > best_score:
                        best_score = score
                        best_match = {
                            'alohacamp_match_id': record['id'],
                            'alohacamp_match_name': aloha_property_name,
                            'alohacamp_score': score,
                            'alohacamp_country': aloha_country,
                            'alohacamp_email': aloha_email,
                            'alohacamp_province': aloha_province
                        }
            
            result = (best_match is not None, best_match or {})
            with self.cache_lock:
                self.aloha_cache[cache_key] = result
            time.sleep(0.1)  # Rate limiting
            return result
            
        except Exception as e:
            self.logger.warning(f"Error checking AlohaCamp: {e}")
            return False, {}

    def update_lead_in_supabase(self, lead: Dict, results: Dict) -> bool:
        """Update lead with duplicate check results in Supabase"""
        try:
            url = f"{self.supabase_url}/rest/v1/contacts_grid_view"
            params = {"id": f"eq.{lead['id']}"}
            
            # Prepare update data
            update_data = {
                "duplicate_check_completed_at": datetime.now().isoformat(),
                "duplicate_check_decision": results.get('decision_reason', 'no_match')
            }
            
            # Add contact data if found
            if results.get('contact_match_type') != 'none':
                update_data.update({
                    "hubspot_contact_match_type": results.get('contact_match_type', 'none'),
                    "hubspot_contact_id": results.get('contact_id', ''),
                    "hubspot_contact_email": results.get('contact_email_hs', ''),
                    "hubspot_contact_phone": results.get('contact_phone_hs', ''),
                    "already_in_pipeline": results.get('already_in_pipeline', False)
                })
            
            # Add deal data if found
            if results.get('deal_match'):
                update_data.update({
                    "hubspot_deal_id": results.get('deal_id', ''),
                    "hubspot_deal_name": results.get('dealname', ''),
                    "hubspot_deal_score": results.get('deal_score', 0),
                    "hubspot_deal_stage": results.get('dealstage', ''),
                    "already_in_pipeline": True
                })
            
            # Add AlohaCamp data if found
            if results.get('exists_on_alohacamp'):
                update_data.update({
                    "exists_on_alohacamp": True,
                    "alohacamp_match_id": results.get('alohacamp_match_id', ''),
                    "alohacamp_match_name": results.get('alohacamp_match_name', ''),
                    "alohacamp_score": results.get('alohacamp_score', 0)
                })
            else:
                update_data["exists_on_alohacamp"] = False
            
            response = requests.patch(url, headers=self.supabase_headers, params=params, json=update_data)
            response.raise_for_status()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error updating lead {lead['id']}: {e}")
            return False

    def process_lead(self, lead: Dict, index: int) -> Dict:
        """Process a single lead for duplicates"""
        if (index + 1) % self.log_every == 0:
            self.logger.info(f"🔍 Processing lead {index + 1}: {lead.get('property_name', 'Unknown')[:50]}")
        
        # Search for contact
        contact_match_type, contact_data = self.search_hubspot_contact(lead)
        
        # Search for deals
        deal_match, deal_data = self.search_hubspot_deals(lead)
        
        # Check AlohaCamp
        aloha_exists, aloha_data = self.check_alohacamp_existence(lead)
        
        # Determine if already in pipeline
        already_in_pipeline = contact_match_type != 'none' or deal_match
        
        # Build decision reasons
        reasons = []
        if contact_match_type != 'none':
            reasons.append(f"contact_{contact_match_type}")
        if deal_match:
            reasons.append(f"deal_score_{deal_data.get('deal_score', 0)}")
        if aloha_exists:
            reasons.append(f"aloha_exists")
        
        # Combine all results
        result = {
            **lead,  # Original lead data
            'contact_match_type': contact_match_type,
            **contact_data,
            'deal_match': deal_match,
            **deal_data,
            'already_in_pipeline': already_in_pipeline,
            'exists_on_alohacamp': aloha_exists,
            **aloha_data,
            'decision_reason': ','.join(reasons) if reasons else 'no_match'
        }
        
        return result

    def process_lead_batch(self, leads_batch: List[Dict], batch_start_index: int) -> Tuple[List[Dict], int, int]:
        """Process a batch of leads in parallel"""
        batch_success = 0
        batch_errors = 0
        processed_results = []
        failed_lead_ids = []  # Track leads that failed to update
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit leads with staggered start times to avoid rate limits
            future_to_lead = {}
            for i, lead in enumerate(leads_batch):
                # Add small delay between submissions to spread out API calls
                if i > 0:
                    time.sleep(0.1)
                future = executor.submit(self.process_lead, lead, batch_start_index + i)
                future_to_lead[future] = lead
            
            # Process completed futures
            for future in as_completed(future_to_lead):
                lead = future_to_lead[future]
                try:
                    result = future.result()
                    
                    # Update in Supabase
                    if self.update_lead_in_supabase(lead, result):
                        batch_success += 1
                    else:
                        batch_errors += 1
                        failed_lead_ids.append(lead['id'])  # Track failed lead
                    
                    processed_results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"Error processing lead {lead.get('id')}: {e}")
                    batch_errors += 1
                    failed_lead_ids.append(lead['id'])  # Track failed lead
        
        # Unmark failed leads so they can be processed in the next run
        if failed_lead_ids:
            self.logger.info(f"🔄 Unmarking {len(failed_lead_ids)} failed leads for retry...")
            self.unmark_leads_as_fetched(failed_lead_ids)
        
        return processed_results, batch_success, batch_errors

    def run(self):
        """Run the complete duplicate check process with parallel processing"""
        start_time = time.time()
        
        self.logger.info("🚀 Starting HubSpot Duplicate Check - Render Version (Parallel)")
        self.logger.info(f"📊 Batch size: {self.batch_size}, Max batches: {self.max_batches}")
        self.logger.info(f"⚡ Parallel workers: {self.max_workers}")
        
        # Check initial unprocessed count
        initial_unprocessed = self.get_unprocessed_leads_count()
        self.logger.info(f"📋 Initial unprocessed leads: {initial_unprocessed:,}")
        
        if initial_unprocessed == 0:
            self.logger.info("✅ No unprocessed leads found - all leads have been processed!")
            return {
                'total_processed': 0,
                'successful': 0,
                'errors': 0,
                'elapsed_time': 0,
                'initial_unprocessed': 0,
                'remaining_unprocessed': 0
            }
        
        total_processed = 0
        total_success = 0
        total_errors = 0
        
        for batch_num in range(1, self.max_batches + 1):
            batch_start_time = time.time()
            self.logger.info(f"\n🔄 Processing Batch {batch_num}/{self.max_batches}")
            
            offset = (batch_num - 1) * self.batch_size
            
            # Get leads for this batch
            leads = self.get_unprocessed_leads(self.batch_size, offset)
            
            if not leads:
                self.logger.info(f"✅ No more leads to process in batch {batch_num}")
                break
            
            # Process leads in parallel
            self.logger.info(f"⚡ Processing {len(leads)} leads with {self.max_workers} parallel workers...")
            
            processed_results, batch_success, batch_errors = self.process_lead_batch(leads, (batch_num - 1) * self.batch_size)
            
            total_processed += len(leads)
            total_success += batch_success
            total_errors += batch_errors
            
            batch_elapsed = time.time() - batch_start_time
            self.logger.info(f"✅ Batch {batch_num} completed: {batch_success} success, {batch_errors} errors")
            self.logger.info(f"⏱️ Batch {batch_num} time: {batch_elapsed:.1f} seconds")
            self.logger.info(f"📊 Batch {batch_num} rate: {len(leads)/batch_elapsed:.1f} leads/second")
        
        elapsed = time.time() - start_time
        
        # Check remaining unprocessed count
        remaining_unprocessed = self.get_unprocessed_leads_count()
        
        # Final summary
        self.logger.info(f"\n🎉 FINAL SUMMARY:")
        self.logger.info(f"   Initial unprocessed: {initial_unprocessed:,}")
        self.logger.info(f"   Total processed: {total_processed}")
        self.logger.info(f"   Successful updates: {total_success}")
        self.logger.info(f"   Errors: {total_errors}")
        self.logger.info(f"   Remaining unprocessed: {remaining_unprocessed:,}")
        self.logger.info(f"   Success rate: {total_success/total_processed*100:.1f}%" if total_processed > 0 else "   Success rate: 0%")
        self.logger.info(f"   Total time elapsed: {elapsed:.1f} seconds")
        self.logger.info(f"   Overall rate: {total_processed/elapsed:.1f} leads/second")
        
        if remaining_unprocessed == 0:
            self.logger.info("🎯 ALL LEADS PROCESSED! No more unprocessed leads remaining.")
        elif remaining_unprocessed < initial_unprocessed:
            progress_percent = ((initial_unprocessed - remaining_unprocessed) / initial_unprocessed) * 100
            self.logger.info(f"📈 Progress: {progress_percent:.1f}% of leads completed")
        
        return {
            'total_processed': total_processed,
            'successful': total_success,
            'errors': total_errors,
            'elapsed_time': elapsed,
            'initial_unprocessed': initial_unprocessed,
            'remaining_unprocessed': remaining_unprocessed
        }

def main():
    """Main entry point"""
    try:
        checker = HubSpotDuplicateChecker()
        results = checker.run()
        
        # Exit with appropriate code
        success_rate = results['successful'] / results['total_processed'] if results['total_processed'] > 0 else 0
        
        if success_rate >= 0.95:  # 95% success rate or higher is considered successful
            print(f"✅ Successfully processed {results['successful']}/{results['total_processed']} leads ({success_rate*100:.1f}% success rate)")
            sys.exit(0)
        elif results['successful'] > 0:
            print(f"⚠️ Processed {results['successful']} leads with {results['errors']} errors ({success_rate*100:.1f}% success rate)")
            sys.exit(1)
        else:
            print("❌ Failed to process any leads")
            sys.exit(2)
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(2)

if __name__ == "__main__":
    main()
