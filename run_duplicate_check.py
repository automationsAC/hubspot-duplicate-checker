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

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    from pathlib import Path
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        print(f"[OK] Loaded environment from .env")
except ImportError:
    pass  # dotenv not available, assume env vars are set

# Import domain blocking
try:
    from shared.domain_blocking import is_domain_blocked
except ImportError:
    # Fallback if shared module not available
    def is_domain_blocked(email: str) -> tuple[bool, str]:
        if not email:
            return False, ''
        email_lower = email.lower().strip()
        if email_lower == 'n/a' or email_lower == 'na':
            return True, 'blocked_email_pattern:n/a'
        return False, ''

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
        # Environment variables (check multiple possible names)
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_api_key = (
            os.environ.get('SUPABASE_API_KEY') or 
            os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or 
            os.environ.get('SUPABASE_ANON_KEY')
        )
        self.hubspot_token = os.environ.get('HUBSPOT_TOKEN')
        self.airtable_token = os.environ.get('AIRTABLE_TOKEN')
        
        if not all([self.supabase_url, self.supabase_api_key, self.hubspot_token]):
            raise ValueError("Missing required environment variables: SUPABASE_URL, SUPABASE_API_KEY (or SUPABASE_SERVICE_ROLE_KEY), HUBSPOT_TOKEN")
        
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
        self.search_api_limit = 4  # Optimized: 4 requests per second (actual limit is 5, leaving buffer)
        
        # Thread-safe rate limiting
        self.crm_api_lock = threading.Lock()
        self.search_api_lock = threading.Lock()
        self.crm_api_calls = []
        self.crm_api_limit = 90  # Optimized: 90 requests per 10 seconds (actual limit is 100, leaving buffer)
        
        # Caching
        self.contact_cache = {}
        self.deal_cache = {}
        self.aloha_cache = {}
        self.cache_lock = threading.Lock()
        
        # Parallel processing configuration
        # Optimized: 4 workers to use Search API capacity (4 req/s) while staying under CRM API limit (8 req/s)
        # Each worker makes ~1.5 CRM calls on average, so 4 workers = ~6 req/s (safe margin)
        self.max_workers = 4
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def wait_for_crm_api_rate_limit(self):
        """Ensure we don't exceed CRM API rate limit (configured limit, actual HubSpot limit is 100 req/10s)"""
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
        """Ensure we don't exceed Search API rate limit (configured limit, actual HubSpot limit is 5 req/s)"""
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
            url = f"{self.supabase_url}/rest/v1/lead_pipeline_view"
            params = {
                "select": "property_uuid",
                "email": "not.is.null",
                "property_name": "not.is.null",
                "duplicate_check_completed_at": "is.null",
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
            url = f"{self.supabase_url}/rest/v1/lead_pipeline_view"
            params = {
                "select": "property_uuid,host_uuid,email,first_name,last_name,property_name,country,phone,booking_url",
                "email": "not.is.null",
                "property_name": "not.is.null",
                "duplicate_check_completed_at": "is.null",
                "limit": str(batch_size)
            }
            
            if offset > 0:
                params["offset"] = str(offset)
                self.logger.info(f"⚠️ Using offset {offset}")
            
            response = requests.get(url, headers=self.supabase_headers, params=params)
            response.raise_for_status()
            
            leads = response.json()
            self.logger.info(f"✅ Retrieved {len(leads)} leads")
            
            # Convert property_uuid to id for compatibility
            for lead in leads:
                lead['id'] = lead.get('property_uuid', '')
            
            return leads
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching leads: {e}")
            return []
    
    # Note: mark_leads_as_fetched and unmark_leads_as_fetched methods removed
    # The new schema uses duplicate_check_completed_at to track processed leads
    # No need to mark as "fetched" separately

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
                'pl': 'pl', 'poland': 'pl',
                'de': 'de', 'germany': 'de',
                'es': 'es', 'spain': 'es',
                'hr': 'hr', 'croatia': 'hr',
                'it': 'it', 'italy': 'it'
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
        """Check if property or host exists in AlohaCamp (Supabase + Airtable)"""
        # First check Supabase (hosts and properties tables) - only if configured
        # Skip silently if not configured (no 401 errors logged)
        try:
            from shared.database import Database
            db = Database()
            
            # Only check Supabase if we have a separate AlohaCamp key configured
            alohacamp_key = os.environ.get('ALOHACAMP_SUPABASE_KEY')
            if alohacamp_key and alohacamp_key != db.supabase_key:
                property_exists = False
                host_exists = False
                property_uuid = None
                host_uuid = None
                
                # Check Properties table in AlohaCamp Supabase
                if lead.get('property_name') and lead.get('country'):
                    property_exists, property_uuid = db.check_property_exists(
                        lead['property_name'],
                        lead['country']
                    )
                
                # Check Hosts table in AlohaCamp Supabase
                if lead.get('email') or lead.get('phone'):
                    host_exists, host_uuid = db.check_host_exists(
                        lead.get('email'),
                        lead.get('phone')
                    )
                
                # If found in Supabase, return immediately
                if property_exists or host_exists:
                    result_data = {
                        'alohacamp_match_id': property_uuid or host_uuid,
                        'alohacamp_match_name': lead.get('property_name', '') if property_exists else '',
                        'alohacamp_source': 'supabase',
                        'property_exists': property_exists,
                        'host_exists': host_exists
                    }
                    return True, result_data
        except Exception as e:
            # Silently continue to Airtable if Supabase check fails
            pass
        
        # Fallback: Check Airtable for properties (legacy)
        if not self.airtable_token:
            return False, {}
        
        property_name = lead.get('property_name', '').strip()
        if not property_name:
            return False, {}
        
        cache_key = f"aloha_airtable_{self.normalize_text(property_name)}"
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
                            'alohacamp_province': aloha_province,
                            'alohacamp_source': 'airtable'
                        }
            
            result = (best_match is not None, best_match or {})
            with self.cache_lock:
                self.aloha_cache[cache_key] = result
            time.sleep(0.1)  # Rate limiting
            return result
            
        except Exception as e:
            self.logger.warning(f"Error checking AlohaCamp Airtable: {e}")
            return False, {}

    def update_lead_in_supabase(self, lead: Dict, results: Dict) -> bool:
        """Update lead with duplicate check results in Supabase using database module"""
        try:
            # Use the shared database module for consistency
            from shared.database import Database
            db = Database()
            
            property_uuid = lead.get('property_uuid') or lead.get('id')
            host_uuid = lead.get('host_uuid')
            
            if not property_uuid:
                self.logger.error(f"❌ No property_uuid found for lead")
                return False
            
            # Prepare result dict in format expected by database module
            db_result = {
                'already_in_pipeline': results.get('already_in_pipeline', False),
                'exists_on_alohacamp': results.get('exists_on_alohacamp', False),
                'decision_reason': results.get('decision_reason', 'no_match'),
                'domain_blocked': results.get('domain_blocked', False)
            }
            
            # Add domain rules check
            if results.get('domain_blocked'):
                db_result['domain_rules_check'] = 'blocked'
            
            return db.update_hubspot_check_result(property_uuid, host_uuid, db_result)
            
        except Exception as e:
            self.logger.error(f"❌ Error updating lead {lead.get('id', 'unknown')}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def process_lead(self, lead: Dict, index: int) -> Dict:
        """Process a single lead for duplicates"""
        if (index + 1) % self.log_every == 0:
            self.logger.info(f"🔍 Processing lead {index + 1}: {lead.get('property_name', 'Unknown')[:50]}")
        
        # Check domain blocking first
        email = lead.get('email', '')
        is_blocked, block_reason = is_domain_blocked(email)
        if is_blocked:
            self.logger.info(f"[BLOCKED] Lead blocked by domain rules: {email} - {block_reason}")
            return {
                **lead,
                'contact_match_type': 'none',
                'deal_match': False,
                'already_in_pipeline': True,  # Blocked leads are treated as "in pipeline"
                'exists_on_alohacamp': False,
                'decision_reason': f"domain_blocked: {block_reason}",
                'domain_blocked': True,
                'block_reason': block_reason
            }
        
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
            # Submit leads to thread pool (rate limiting handled per-API-call, not per-submission)
            future_to_lead = {}
            for i, lead in enumerate(leads_batch):
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
        
        # Note: Failed leads will be retried automatically on next run since we don't mark as fetched
        
        return processed_results, batch_success, batch_errors

    def run(self):
        """Run the complete duplicate check process with parallel processing"""
        start_time = time.time()
        
        self.logger.info("🚀 Starting HubSpot Duplicate Check - Render Version (Parallel)")
        self.logger.info(f"📊 Batch size: {self.batch_size}, Max batches: {self.max_batches}")
        self.logger.info(f"⚡ Parallel workers: {self.max_workers}")
        
        # Check initial unprocessed count
        initial_unprocessed = self.get_unprocessed_leads_count()
        self.logger.info(f"📊 DATABASE STATUS CHECK:")
        self.logger.info(f"   📋 Total unprocessed leads in database: {initial_unprocessed:,}")
        
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
        
        # Calculate how many leads we can process in this run
        max_can_process = self.batch_size * self.max_batches
        actual_will_process = min(initial_unprocessed, max_can_process)
        
        self.logger.info(f"🎯 PROCESSING PLAN:")
        self.logger.info(f"   📦 Batch size: {self.batch_size:,}")
        self.logger.info(f"   🔄 Max batches: {self.max_batches}")
        self.logger.info(f"   📊 Max can process this run: {max_can_process:,}")
        self.logger.info(f"   🎯 Will actually process: {actual_will_process:,}")
        
        if actual_will_process < initial_unprocessed:
            remaining_after_run = initial_unprocessed - actual_will_process
            self.logger.info(f"   ⏳ Will remain unprocessed: {remaining_after_run:,} (next run)")
        else:
            self.logger.info(f"   🎯 Will process ALL remaining leads!")
        
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
        self.logger.info(f"   📋 Unprocessed leads in DB (start): {initial_unprocessed:,}")
        self.logger.info(f"   📊 Leads processed this run: {total_processed}")
        self.logger.info(f"   ✅ Successful updates: {total_success}")
        self.logger.info(f"   ❌ Errors: {total_errors}")
        self.logger.info(f"   📋 Unprocessed leads in DB (end): {remaining_unprocessed:,}")
        self.logger.info(f"   📈 Success rate: {total_success/total_processed*100:.1f}%" if total_processed > 0 else "   Success rate: 0%")
        self.logger.info(f"   ⏱️ Total time elapsed: {elapsed:.1f} seconds")
        self.logger.info(f"   🚀 Overall rate: {total_processed/elapsed:.1f} leads/second")
        
        if remaining_unprocessed == 0:
            self.logger.info("🎯 ALL LEADS PROCESSED! Database is now fully processed.")
        elif remaining_unprocessed < initial_unprocessed:
            progress_percent = ((initial_unprocessed - remaining_unprocessed) / initial_unprocessed) * 100
            self.logger.info(f"📈 Database progress: {progress_percent:.1f}% of total leads completed")
            self.logger.info(f"🔄 Next run will process the remaining {remaining_unprocessed:,} leads")
        
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
        
        # If there were no leads to process initially, this is not an error
        if results.get('initial_unprocessed', 0) == 0:
            print("✅ No unprocessed leads found - all leads have been processed!")
            sys.exit(0)
        
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
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)

if __name__ == "__main__":
    main()
