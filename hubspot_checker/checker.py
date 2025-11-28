#!/usr/bin/env python3
"""
HubSpot Lead Checker - Adapted for Instantly/Supabase Integration
Checks for duplicates in HubSpot (Contacts & Deals) and optionally AlohaCamp
"""

import os
import csv
import json
import time
import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import re
from shared.domain_blocking import is_domain_blocked

try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    print("⚠️  rapidfuzz not available, installing...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'rapidfuzz', '--break-system-packages'])
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True

class HubSpotLeadChecker:
    def __init__(self):
        self.hubspot_token = os.environ.get('HUBSPOT_TOKEN')
        if not self.hubspot_token:
            raise ValueError("HUBSPOT_TOKEN environment variable is required")
        
        self.hubspot_headers = {
            'Authorization': f'Bearer {self.hubspot_token}',
            'Content-Type': 'application/json'
        }
        
        # Configuration
        self.sample_size = int(os.environ.get('SAMPLE_SIZE', 100))
        self.log_every = int(os.environ.get('LOG_EVERY', 10))
        self.filter_property = os.environ.get('FILTER_PROPERTY_CONTAINS', '').lower()
        self.offset = int(os.environ.get('OFFSET', 0))
        
        # AlohaCamp/Airtable config (optional)
        self.airtable_token = os.environ.get('AIRTABLE_TOKEN')
        self.aloha_base = os.environ.get('ALOHA_BASE')
        self.aloha_table = os.environ.get('ALOHA_TABLE')
        self.aloha_view = os.environ.get('ALOHA_VIEW')
        
        # Setup logging
        self.setup_logging()
        
        # Caching for efficiency
        self.contact_cache = {}
        self.deal_cache = {}
        self.aloha_cache = {}
        
        # Rate limiting tracking for Search API (5 requests/second)
        self.search_api_calls = []
        self.search_api_limit = 5  # requests per second
        self.last_search_call = 0
        
    def setup_logging(self):
        """Setup logging to file and console"""
        import tempfile
        log_dir = os.path.join(tempfile.gettempdir(), 'hubspot_check')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'hubspot_check.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def wait_for_search_api_rate_limit(self):
        """Ensure we don't exceed 5 requests/second for Search API"""
        current_time = time.time()
        
        # Remove calls older than 1 second
        self.search_api_calls = [call_time for call_time in self.search_api_calls if current_time - call_time < 1.0]
        
        # If we've made 5 calls in the last second, wait
        if len(self.search_api_calls) >= self.search_api_limit:
            wait_time = 1.0 - (current_time - self.search_api_calls[0])
            if wait_time > 0:
                self.logger.info(f"⏳ Rate limiting: waiting {wait_time:.2f}s for Search API")
                time.sleep(wait_time)
        
        # Record this call
        self.search_api_calls.append(current_time)

    def load_leads(self) -> List[Dict]:
        """Load leads from the prepared CSV file"""
        # Find the most recent leads file
        import glob
        lead_files = glob.glob('leads_for_checking_*.csv')
        if not lead_files:
            raise FileNotFoundError("No leads_for_checking_*.csv file found. Run get_detailed_null_leads.py first.")
        
        # Use the most recent file
        latest_file = max(lead_files, key=os.path.getctime)
        self.logger.info(f"📄 Loading leads from: {latest_file}")
        
        leads = []
        with open(latest_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                leads.append(row)
        
        self.logger.info(f"📊 Loaded {len(leads)} leads")
        
        # Apply sampling and filtering
        if self.filter_property:
            filtered_leads = [
                lead for lead in leads 
                if self.filter_property in lead.get('property_name', '').lower()
            ]
            self.logger.info(f"🔍 Filtered to {len(filtered_leads)} leads containing '{self.filter_property}'")
            leads = filtered_leads
        
        # Apply offset to skip already processed leads
        if self.offset > 0:
            leads = leads[self.offset:]
            self.logger.info(f"🔄 Skipping first {self.offset} leads (already processed)")
        
        if len(leads) > self.sample_size:
            leads = leads[:self.sample_size]
            self.logger.info(f"📊 Sampling {self.sample_size} leads (from offset {self.offset})")
        
        return leads

    def normalize_text(self, text: str) -> str:
        """Normalize text for comparison"""
        if not text:
            return ''
        
        # Remove diacritics and normalize
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

    def search_hubspot_contact(self, lead: Dict) -> Tuple[Optional[str], Dict]:
        """Search for contact in HubSpot by email or phone"""
        email = lead.get('email', '').strip().lower()
        phone = self.normalize_phone(lead.get('phone', ''))
        
        # Try email first
        if email:
            cache_key = f"contact_email_{email}"
            if cache_key in self.contact_cache:
                return self.contact_cache[cache_key]
            
            try:
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
                        self.contact_cache[cache_key] = result
                        return result
                
                time.sleep(0.15)  # CRM API: 100 requests per 10 seconds = 0.1s minimum + buffer
                
            except Exception as e:
                self.logger.warning(f"Error searching contact by email: {e}")
        
        # Try phone if email didn't work
        if phone:
            cache_key = f"contact_phone_{phone}"
            if cache_key in self.contact_cache:
                return self.contact_cache[cache_key]
            
            try:
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
                        self.contact_cache[cache_key] = result
                        return result
                
                time.sleep(0.15)  # CRM API: 100 requests per 10 seconds = 0.1s minimum + buffer
                
            except Exception as e:
                self.logger.warning(f"Error searching contact by phone: {e}")
        
        return ('none', {
            'contact_id': '',
            'contact_name': '',
            'contact_email_hs': '',
            'contact_phone_hs': ''
        })

    def search_hubspot_deals(self, lead: Dict) -> Tuple[bool, Dict]:
        """Search for deals in HubSpot using fuzzy matching"""
        property_name = lead.get('property_name', '').strip()
        if not property_name:
            return False, {}
        
        # Normalize property name for search
        normalized_property = self.normalize_text(property_name)
        search_terms = normalized_property.split()[:3]  # Use top 3 words
        
        cache_key = f"deal_{normalized_property}"
        if cache_key in self.deal_cache:
            return self.deal_cache[cache_key]
        
        try:
            # Respect Search API rate limit (5 requests/second)
            self.wait_for_search_api_rate_limit()
            
            url = "https://api.hubapi.com/crm/v3/objects/deals/search"
            
            # Use free-text search to avoid token issues
            search_query = ' '.join(search_terms)
            payload = {
                "query": search_query,
                "limit": 20,
                "properties": ["dealname", "dealstage", "country", "city", "address", "booking_url"]
            }
            
            response = requests.post(url, headers=self.hubspot_headers, json=payload)
            
            if response.status_code == 429:
                # Rate limited - HubSpot Search API limit is 5 requests/second
                self.logger.warning(f"Rate limited (429), waiting 15 seconds before retry...")
                time.sleep(15)  # Longer wait for rate limit reset
                
                # Retry once after rate limit
                response = requests.post(url, headers=self.hubspot_headers, json=payload)
                if response.status_code == 429:
                    self.logger.warning(f"Still rate limited after retry, waiting 30 seconds...")
                    time.sleep(30)  # Even longer wait
                    return False, {}
            
            if response.status_code != 200:
                self.logger.warning(f"Deal search failed: {response.status_code}")
                time.sleep(2)  # Longer wait between failed requests
                return False, {}
            
            data = response.json()
            best_match = None
            best_score = 0
            
            for deal in data.get('results', []):
                deal_name = deal['properties'].get('dealname', '')
                if not deal_name:
                    continue
                
                # Calculate fuzzy scores - use AVERAGE instead of MAX
                token_set_score = fuzz.token_set_ratio(normalized_property, self.normalize_text(deal_name))
                partial_token_score = fuzz.partial_token_sort_ratio(normalized_property, self.normalize_text(deal_name))
                score = (token_set_score + partial_token_score) / 2  # Average instead of max
                
                # Check word count - for 100% matches with word diff, require location match
                lead_words = len(normalized_property.split())
                deal_words = len(self.normalize_text(deal_name).split())
                word_count_match = (lead_words == deal_words)
                
                # Check location match
                location_match, location_details = self.check_location_match(lead, deal)
                
                # Scoring logic with special 100% rule
                is_strong = score >= 92
                is_medium = 85 <= score < 92
                is_location_ok = location_match
                
                accept_match = False
                special_rule_applied = False  # Flag to skip other rules
                
                # Special rule: For 100% name matches with word count mismatch, REQUIRE URL or CITY match
                # This prevents "Oasis" matching "Oasis Rural"
                if score >= 99.5 and not word_count_match:
                    special_rule_applied = True
                    
                    # Cascade: URL → City → Reject
                    lead_url = (lead.get('booking_url', '') or '').strip()
                    deal_url = (deal['properties'].get('booking_url', '') or '').strip()
                    
                    # 1. Check URL first (strongest signal)
                    if lead_url and deal_url:
                        # Extract booking.com slug for comparison
                        lead_slug_match = re.search(r'booking\.com/hotel/[^/]+/([^\.?]+)', lead_url)
                        deal_slug_match = re.search(r'booking\.com/hotel/[^/]+/([^\.?]+)', deal_url)
                        
                        if lead_slug_match and deal_slug_match:
                            lead_slug = lead_slug_match.group(1).lower()
                            deal_slug = deal_slug_match.group(1).lower()
                            if lead_slug == deal_slug:
                                accept_match = True  # OK - URL matches!
                            else:
                                accept_match = False  # REJECT - different URLs
                        else:
                            # URL exists but can't parse - check city
                            lead_city = (lead.get('city', '') or '').strip()
                            deal_city = (deal['properties'].get('city', '') or '').strip()
                            
                            if lead_city and deal_city:
                                city_score = fuzz.ratio(lead_city.lower(), deal_city.lower())
                                accept_match = (city_score >= 90)
                            else:
                                accept_match = False  # REJECT - no city
                    else:
                        # 2. No URL - check city
                        lead_city = (lead.get('city', '') or '').strip()
                        deal_city = (deal['properties'].get('city', '') or '').strip()
                        
                        if lead_city and deal_city:
                            city_score = fuzz.ratio(lead_city.lower(), deal_city.lower())
                            accept_match = (city_score >= 90)
                        else:
                            accept_match = False  # REJECT - no URL and no city
                
                # Normal rules - ONLY if special rule wasn't applied
                if not special_rule_applied:
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
            self.deal_cache[cache_key] = result
            # Rate limiting handled by wait_for_search_api_rate_limit() at start
            return result
            
        except Exception as e:
            self.logger.warning(f"Error searching deals: {e}")
            return False, {}

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
            # Normalize country codes/names to 2-letter codes for comparison
            # Note: GitHub repo (https://github.com/automationsAC/hubspot-duplicate-checker) 
            # only has pl/de/es and has a bug in the matching logic. This version fixes it
            # and adds hr/it support.
            country_to_code = {
                'pl': 'pl', 'poland': 'pl',
                'de': 'de', 'germany': 'de',
                'es': 'es', 'spain': 'es',
                'hr': 'hr', 'croatia': 'hr',
                'it': 'it', 'italy': 'it'
            }
            lead_country_norm = country_to_code.get(lead_country, lead_country)
            deal_country_norm = country_to_code.get(deal_country, deal_country)
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

    def check_contact_deal_association(self, contact_id: str, deal_id: str) -> str:
        """Check if contact and deal are associated in HubSpot"""
        if not contact_id or not deal_id:
            return 'n/a'
        
        try:
            url = f"https://api.hubapi.com/crm/v4/objects/contacts/{contact_id}/associations/deals"
            response = requests.get(url, headers=self.hubspot_headers)
            
            if response.status_code == 200:
                data = response.json()
                associated_deals = [assoc['toObjectId'] for assoc in data.get('results', [])]
                return 'true' if deal_id in associated_deals else 'false'
            
            time.sleep(0.1)
            return 'unknown'
            
        except Exception as e:
            self.logger.warning(f"Error checking association: {e}")
            return 'unknown'

    def check_alohacamp_existence(self, lead: Dict) -> Tuple[bool, Dict]:
        """Check if property exists in AlohaCamp (via Supabase properties table)"""
        # Use Supabase instead of Airtable
        supabase_url = os.environ.get('SUPABASE_URL')
        supabase_key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_ANON_KEY') or os.environ.get('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            self.logger.warning("SUPABASE_URL or SUPABASE_KEY not set, skipping AlohaCamp check")
            return False, {}
        
        property_name = lead.get('property_name', '').strip()
        country = lead.get('country', '').strip().lower()
        
        if not property_name:
            return False, {}
        
        # Create cache key based on property name and country
        cache_key = f"aloha_{self.normalize_text(property_name)}_{country}"
        if cache_key in self.aloha_cache:
            return self.aloha_cache[cache_key]
        
        try:
            # Query Supabase properties table for published properties
            url = f"{supabase_url}/rest/v1/properties"
            headers = {
                'apikey': supabase_key,
                'Authorization': f'Bearer {supabase_key}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'select': 'uuid,property_name,country,is_published',
                'is_published': 'eq.true',  # Only check published (live) properties
                'limit': '1000'  # Fetch up to 1000 published properties
            }
            
            # If we have country info, filter by it
            if country:
                params['country'] = f'eq.{country}'
            
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code != 200:
                self.logger.warning(f"Supabase API error: {response.status_code}")
                return False, {}
            
            properties = response.json()
            
            if not properties:
                self.logger.debug(f"No published AlohaCamp properties found for country: {country}")
                result = (False, {})
                self.aloha_cache[cache_key] = result
                return result
            
            self.logger.info(f"Checking '{property_name}' against {len(properties)} published AlohaCamp properties")
            
            best_match = None
            best_score = 0
            
            normalized_property = self.normalize_text(property_name)
            
            for prop in properties:
                aloha_property_name = prop.get('property_name', '')
                aloha_country = prop.get('country', '')
                
                if not aloha_property_name:
                    continue
                
                # Calculate fuzzy match score
                score = fuzz.token_set_ratio(normalized_property, self.normalize_text(aloha_property_name))
                
                if score >= 90:  # 90% similarity threshold
                    # Verify country match if both are available
                    location_ok = True
                    if country and aloha_country:
                        location_ok = country.lower() == aloha_country.lower()
                    
                    if location_ok and score > best_score:
                        best_score = score
                        best_match = {
                            'alohacamp_match_id': prop.get('uuid'),
                            'alohacamp_match_name': aloha_property_name,
                            'alohacamp_score': score,
                            'alohacamp_country': aloha_country,
                            'alohacamp_is_published': prop.get('is_published')
                        }
            
            result = (best_match is not None, best_match or {})
            self.aloha_cache[cache_key] = result
            
            if best_match:
                self.logger.info(f"✅ AlohaCamp match found: '{property_name}' → '{best_match['alohacamp_match_name']}' (score: {best_score})")
            
            return result
            
        except Exception as e:
            self.logger.warning(f"Error checking AlohaCamp via Supabase: {e}")
            import traceback
            traceback.print_exc()
            return False, {}

    def process_lead(self, lead: Dict, index: int) -> Dict:
        """Process a single lead for duplicates"""
        if (index + 1) % self.log_every == 0:
            self.logger.info(f"[*] Processing lead {index + 1}/{self.sample_size}: {lead.get('property_name', 'Unknown')[:50]}")
        
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
        
        # Check association if both found
        association = 'n/a'
        if contact_data.get('contact_id') and deal_data.get('deal_id'):
            association = self.check_contact_deal_association(
                contact_data['contact_id'], 
                deal_data['deal_id']
            )
        
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
            'association_with_contact': association,
            'already_in_pipeline': already_in_pipeline,
            'exists_on_alohacamp': aloha_exists,
            **aloha_data,
            'decision_reason': ','.join(reasons)
        }
        
        return result

    def run_check(self):
        """Run the complete duplicate check process"""
        start_time = time.time()
        
        self.logger.info("🚀 Starting HubSpot Lead Checker")
        self.logger.info(f"📊 Sample size: {self.sample_size}")
        self.logger.info(f"📝 Log every: {self.log_every} leads")
        
        # Load leads
        leads = self.load_leads()
        self.sample_size = min(self.sample_size, len(leads))
        
        # Process leads
        results = []
        for i, lead in enumerate(leads[:self.sample_size]):
            try:
                result = self.process_lead(lead, i)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error processing lead {i}: {e}")
                continue
        
        # DIRECT UPDATE: Update Supabase immediately, bypass CSV issues
        try:
            from direct_supabase_updater import DirectSupabaseUpdater
            updater = DirectSupabaseUpdater()
            update_stats = updater.update_batch_directly(results)
            self.logger.info(f"💾 Direct Supabase updates: {update_stats['success_count']} success, {update_stats['error_count']} errors")
        except Exception as e:
            self.logger.error(f"❌ Direct Supabase update failed: {e}")
            self.logger.info("📄 Falling back to CSV method...")
        
        # Also save CSV results for reference/backup
        self.save_results(results)
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Completed in {elapsed:.1f} seconds")
        self.logger.info(f"📊 Processed {len(results)} leads")
        
        # Summary stats
        self.log_summary_stats(results)

    def save_results(self, results: List[Dict]):
        """Save results to multiple formats"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Enriched CSV with all data
        enriched_file = f"output/enriched_{timestamp}.csv"
        if results:
            # Get all possible fieldnames from all results
            all_fieldnames = set()
            for result in results:
                all_fieldnames.update(result.keys())
            
            with open(enriched_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_fieldnames))
                writer.writeheader()
                writer.writerows(results)
            self.logger.info(f"💾 Saved enriched results: {enriched_file}")
        
        # Summary CSV
        summary_file = f"output/summary_{timestamp}.csv"
        summary_fields = [
            'supabase_id', 'email', 'property_name', 'country',
            'contact_match_type', 'deal_match', 'location_match',
            'already_in_pipeline', 'exists_on_alohacamp', 'decision_reason'
        ]
        
        if results:
            with open(summary_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=summary_fields)
                writer.writeheader()
                for result in results:
                    summary_row = {field: result.get(field, '') for field in summary_fields}
                    writer.writerow(summary_row)
            self.logger.info(f"📋 Saved summary: {summary_file}")
        
        # JSON results
        json_file = f"output/results_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        self.logger.info(f"📄 Saved JSON: {json_file}")

    def log_summary_stats(self, results: List[Dict]):
        """Log summary statistics"""
        if not results:
            return
        
        total = len(results)
        
        # Contact matches
        contact_stats = defaultdict(int)
        for result in results:
            contact_stats[result.get('contact_match_type', 'none')] += 1
        
        # Deal matches
        deal_matches = sum(1 for r in results if r.get('deal_match'))
        
        # Location matches
        location_matches = sum(1 for r in results if r.get('location_match'))
        
        # Already in pipeline
        in_pipeline = sum(1 for r in results if r.get('already_in_pipeline'))
        
        # AlohaCamp exists
        aloha_exists = sum(1 for r in results if r.get('exists_on_alohacamp'))
        
        self.logger.info("📊 **SUMMARY STATISTICS:**")
        self.logger.info(f"   Total processed: {total}")
        self.logger.info(f"   Contact matches: {dict(contact_stats)}")
        self.logger.info(f"   Deal matches: {deal_matches}")
        self.logger.info(f"   Location matches: {location_matches}")
        self.logger.info(f"   Already in pipeline: {in_pipeline} ({in_pipeline/total*100:.1f}%)")
        self.logger.info(f"   Exists on AlohaCamp: {aloha_exists} ({aloha_exists/total*100:.1f}%)")


def main():
    """Main entry point"""
    try:
        checker = HubSpotLeadChecker()
        checker.run_check()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
