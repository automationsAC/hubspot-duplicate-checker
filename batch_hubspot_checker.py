#!/usr/bin/env python3
"""
Batch HubSpot Duplicate Checker
Processes CSV with leads and checks against HubSpot for duplicates
"""

import os
import csv
import time
import json
import re
import unicodedata
import random
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
import requests
from rapidfuzz import fuzz

# Load environment variables
load_dotenv()

class BatchHubSpotChecker:
    def __init__(self):
        self.hubspot_token = os.getenv('HUBSPOT_TOKEN')
        if not self.hubspot_token:
            raise ValueError("HUBSPOT_TOKEN not found in .env")
        
        self.headers = {
            'Authorization': f'Bearer {self.hubspot_token}',
            'Content-Type': 'application/json'
        }
        
        # Rate limiting: HubSpot allows 100 requests per 10 seconds
        self.requests_made = 0
        self.rate_limit_window_start = time.time()
        self.max_requests_per_window = 95  # Optimized limit (5% buffer)
        self.window_duration = 10  # seconds
        
        # Progress tracking
        self.processed_count = 0
        self.matched_count = 0
        self.start_time = datetime.now()
        
    def normalize_text(self, text: str) -> str:
        """Normalize text for comparison (same as checker.py)"""
        if not text:
            return ''
        
        # Remove diacritics
        text = unicodedata.normalize('NFD', text)
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        
        # Lowercase and clean whitespace
        text = re.sub(r'\s+', ' ', text.lower().strip())
        
        # Remove common stop words ONLY for names with 3+ words
        # This prevents "Ferienhaus Waldblick" → "waldblick" (too short!)
        words = text.split()
        if len(words) >= 3:
            stop_words = ['hotel', 'pension', 'ferienwohnung', 'ferienhaus', 
                          'apartment', 'villa', 'resort']
            words = [w for w in words if w not in stop_words]
        
        return ' '.join(words)
    
    def respect_rate_limit(self):
        """Ensure we don't exceed HubSpot rate limits"""
        self.requests_made += 1
        
        # Check if we need to wait
        elapsed = time.time() - self.rate_limit_window_start
        
        if self.requests_made >= self.max_requests_per_window:
            if elapsed < self.window_duration:
                sleep_time = self.window_duration - elapsed + 0.5  # Add buffer
                print(f"  [Rate limit] Sleeping for {sleep_time:.1f}s...")
                time.sleep(sleep_time)
            
            # Reset window
            self.rate_limit_window_start = time.time()
            self.requests_made = 0
    
    def search_hubspot_contact(self, email: str, phone: str) -> tuple:
        """Search for contact in HubSpot by email or phone"""
        # Try email first
        if email and email.lower() not in ['n/a', 'na', '']:
            self.respect_rate_limit()
            
            url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
            payload = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email
                    }]
                }],
                "properties": ["email", "firstname", "lastname", "phone"]
            }
            
            try:
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('results'):
                        contact = data['results'][0]
                        return ('email_exact', {
                            'contact_id': contact['id'],
                            'contact_name': f"{contact['properties'].get('firstname', '')} {contact['properties'].get('lastname', '')}".strip(),
                            'contact_email': contact['properties'].get('email', '')
                        })
            except Exception as e:
                print(f"  [Warning] Contact search by email failed: {e}")
        
        # Try phone if available
        if phone and phone.strip():
            self.respect_rate_limit()
            
            url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
            payload = {
                "filterGroups": [
                    {"filters": [{"propertyName": "phone", "operator": "EQ", "value": phone}]},
                    {"filters": [{"propertyName": "mobilephone", "operator": "EQ", "value": phone}]}
                ],
                "properties": ["email", "firstname", "lastname", "phone"]
            }
            
            try:
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('results'):
                        contact = data['results'][0]
                        return ('phone_exact', {
                            'contact_id': contact['id'],
                            'contact_name': f"{contact['properties'].get('firstname', '')} {contact['properties'].get('lastname', '')}".strip(),
                            'contact_email': contact['properties'].get('email', '')
                        })
            except Exception as e:
                print(f"  [Warning] Contact search by phone failed: {e}")
        
        return ('none', {})
    
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
    
    def extract_domain(self, email: str) -> str:
        """Extract domain from email"""
        if not email or '@' not in email:
            return ''
        return email.split('@')[1].lower()
    
    def normalize_booking_url(self, url: str) -> str:
        """Normalize booking URL for comparison"""
        if not url:
            return ''
        # Extract the hotel slug from booking.com URL
        import re
        match = re.search(r'booking\.com/hotel/[^/]+/([^\.]+)', url)
        if match:
            return match.group(1).lower()
        return url.lower()
    
    def search_hubspot_deals(self, property_name: str, country: str) -> list:
        """Search for deals in HubSpot"""
        self.respect_rate_limit()
        
        url = "https://api.hubapi.com/crm/v3/objects/deals/search"
        
        # Build search query (first 3 words)
        name_words = self.normalize_text(property_name).split()[:3]
        search_query = ' '.join(name_words)
        
        payload = {
            "query": search_query,
            "limit": 20,
            "properties": [
                "dealname", "country", "city", "dealstage",
                "booking_url", "createdate"
            ]
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('results', [])
            else:
                print(f"  [Warning] HubSpot API error: {response.status_code}")
                return []
        except Exception as e:
            print(f"  [Error] Request failed: {e}")
            return []
    
    def check_location_match(self, lead_country: str, lead_city: str, 
                            deal_country: str, deal_city: str) -> tuple[bool, str]:
        """Check if locations match"""
        details = []
        
        # Normalize
        lead_country = (lead_country or '').strip().lower()
        deal_country = (deal_country or '').strip().lower()
        lead_city = self.normalize_text(lead_city or '')
        deal_city = self.normalize_text(deal_city or '')
        
        country_match = False
        city_match = False
        
        if lead_country and deal_country:
            # Country code mapping
            country_codes = {
                'pl': 'pl', 'poland': 'pl', 'polska': 'pl',
                'de': 'de', 'germany': 'de', 'deutschland': 'de',
                'es': 'es', 'spain': 'es', 'españa': 'es', 'espana': 'es',
                'hr': 'hr', 'croatia': 'hr', 'hrvatska': 'hr',
                'it': 'it', 'italy': 'it', 'italia': 'it',
                'fr': 'fr', 'france': 'fr',
                'at': 'at', 'austria': 'at', 'österreich': 'at', 'osterreich': 'at',
                'ch': 'ch', 'switzerland': 'ch', 'schweiz': 'ch',
                'nl': 'nl', 'netherlands': 'nl', 'nederland': 'nl',
                'be': 'be', 'belgium': 'be', 'belgique': 'be',
                'pt': 'pt', 'portugal': 'pt',
                'cz': 'cz', 'czech republic': 'cz', 'czechia': 'cz',
                'sk': 'sk', 'slovakia': 'sk',
                'hu': 'hu', 'hungary': 'hu',
                'ro': 'ro', 'romania': 'ro',
                'bg': 'bg', 'bulgaria': 'bg',
                'gr': 'gr', 'greece': 'gr',
                'si': 'si', 'slovenia': 'si',
                'ee': 'ee', 'estonia': 'ee',
                'lv': 'lv', 'latvia': 'lv',
                'lt': 'lt', 'lithuania': 'lt'
            }
            lead_country_norm = country_codes.get(lead_country, lead_country)
            deal_country_norm = country_codes.get(deal_country, deal_country)
            country_match = lead_country_norm == deal_country_norm
            details.append(f"Country: {lead_country} vs {deal_country} ({country_match})")
        
        if lead_city and deal_city:
            city_score = fuzz.ratio(lead_city, deal_city)
            city_match = city_score >= 90
            details.append(f"City: {lead_city} vs {deal_city} ({city_score}%)")
        
        location_ok = country_match or city_match
        
        return location_ok, '; '.join(details)
    
    def find_best_match(self, lead: Dict[str, str]) -> Dict[str, Any]:
        """Find best matching deal for a lead using cascade matching strategy"""
        property_name = lead['property_name']
        country = lead['country']
        city = lead['city']
        email = lead['email']
        booking_url = lead['booking_url']
        phone = self.normalize_phone(lead.get('phone', ''))
        
        # Step 1: Check if contact exists by email/phone
        contact_match_type, contact_data = self.search_hubspot_contact(email, phone)
        
        if contact_match_type != 'none':
            return {
                'match_found': True,
                'match_type': f'contact_{contact_match_type}',
                'deals_checked': 0,
                'best_score': 100,
                'contact_id': contact_data.get('contact_id', ''),
                'contact_name': contact_data.get('contact_name', ''),
                'deal_id': '',
                'deal_name': '',
                'deal_stage': '',
                'location_match': True,
                'location_details': f'Contact matched by {contact_match_type}',
                'comment': f'Contact found by {contact_match_type}'
            }
        
        # Step 2: Search for deals
        deals = self.search_hubspot_deals(property_name, country)
        
        if not deals:
            return {
                'match_found': False,
                'match_type': 'none',
                'deals_checked': 0,
                'comment': 'No contact or deals found in HubSpot'
            }
        
        # Step 3: Match deals using multiple signals
        normalized_property = self.normalize_text(property_name)
        lead_booking_slug = self.normalize_booking_url(booking_url)
        lead_email_domain = self.extract_domain(email)
        
        best_score = 0
        best_match = None
        match_signals = []
        
        for deal in deals:
            deal_name = deal['properties'].get('dealname', '')
            if not deal_name:
                continue
            
            signals = []
            combined_score = 0
            
            # Signal 1: Booking URL (strongest signal)
            deal_booking_url = deal['properties'].get('booking_url', '')
            deal_booking_slug = self.normalize_booking_url(deal_booking_url)
            if lead_booking_slug and deal_booking_slug and lead_booking_slug == deal_booking_slug:
                signals.append('url_exact')
                combined_score += 100  # Perfect match
            
            # Signal 2: City match
            deal_country = deal['properties'].get('country', '')
            deal_city = deal['properties'].get('city', '')
            location_match, location_details = self.check_location_match(
                country, city, deal_country, deal_city
            )
            if location_match and city and deal_city:
                city_score = fuzz.ratio(self.normalize_text(city), self.normalize_text(deal_city))
                if city_score >= 90:
                    signals.append(f'city_match_{city_score}')
                    combined_score += 40
            
            # Signal 3: Property name fuzzy match (average of two methods)
            token_set_score = fuzz.token_set_ratio(
                normalized_property, 
                self.normalize_text(deal_name)
            )
            partial_token_score = fuzz.partial_token_sort_ratio(
                normalized_property,
                self.normalize_text(deal_name)
            )
            name_score = (token_set_score + partial_token_score) / 2  # Average instead of max
            
            # Check word count difference
            lead_words = len(normalized_property.split())
            deal_words = len(self.normalize_text(deal_name).split())
            word_count_match = (lead_words == deal_words)
            
            signals.append(f'name_fuzzy_{int(name_score)}')
            combined_score += name_score * 0.6  # Weight: 60% of name score
            
            # Signal 4: Country match (bonus)
            if country and deal_country and country.upper() == deal_country.upper():
                signals.append('country_match')
                combined_score += 10
            
            # Decide if this is a good match
            # - URL match = instant accept
            # - City + name strong = accept
            # - Name very strong = accept
            accept_match = False
            has_url = 'url_exact' in signals
            has_city = any('city_match' in s for s in signals)
            
            # Special rule: For 100% name matches with word count mismatch, REQUIRE URL or City
            if name_score == 100 and not word_count_match:
                if has_url or has_city:
                    accept_match = True
                else:
                    accept_match = False  # REJECT - prevents "Oasis" matching "Oasis Rural"
            elif has_url:
                accept_match = True
            elif combined_score >= 90:
                accept_match = True
            elif name_score >= 92 and location_match:
                accept_match = True
            
            if accept_match and combined_score > best_score:
                best_score = combined_score
                best_match = {
                    'deal_id': deal['id'],
                    'deal_name': deal_name,
                    'deal_score': int(combined_score),
                    'name_score': name_score,
                    'location_match': location_match,
                    'location_details': location_details,
                    'deal_stage': deal['properties'].get('dealstage', ''),
                    'deal_country': deal_country,
                    'deal_city': deal_city,
                    'signals': ', '.join(signals)
                }
        
        if best_match:
            return {
                'match_found': True,
                'match_type': 'deal',
                'deals_checked': len(deals),
                'best_score': best_match['deal_score'],
                'name_score': best_match['name_score'],
                'signals': best_match['signals'],
                'contact_id': '',
                'contact_name': '',
                'deal_id': best_match['deal_id'],
                'deal_name': best_match['deal_name'],
                'deal_stage': best_match['deal_stage'],
                'location_match': best_match['location_match'],
                'location_details': best_match['location_details'],
                'comment': f"Deal matched: {best_match['signals']}"
            }
        else:
            return {
                'match_found': False,
                'match_type': 'none',
                'deals_checked': len(deals),
                'best_score': int(best_score) if best_score > 0 else 0,
                'comment': f"No good match ({len(deals)} deals checked, best combined score: {int(best_score)})"
            }
    
    def process_csv(self, input_file: str, output_file: str, log_every: int = 100, limit: int = None):
        """Process the CSV file and save results"""
        print(f"Starting batch processing...")
        print(f"Input: {input_file}")
        print(f"Output: {output_file}")
        print(f"Rate limit: {self.max_requests_per_window} requests per {self.window_duration}s")
        if limit:
            print(f"TEST MODE: Processing only first {limit} leads")
        print("-" * 80)
        
        # Read input CSV
        with open(input_file, 'r', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            input_rows = list(reader)
        
        # Apply limit if specified
        if limit:
            input_rows = input_rows[:limit]
        
        total_rows = len(input_rows)
        print(f"Total leads to process: {total_rows}")
        print("-" * 80)
        
        # Track leads without matches for human verification
        no_match_leads = []
        
        # Prepare output file
        output_fieldnames = [
            'property_uuid', 'property_name', 'country', 'city', 'email', 'booking_url',
            'match_found', 'match_type', 'deals_checked', 'best_score', 'name_score', 'signals',
            'contact_id', 'contact_name', 'deal_id', 'deal_name', 
            'deal_stage', 'location_match', 'location_details', 'comment'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=output_fieldnames)
            writer.writeheader()
            
            for idx, row in enumerate(input_rows, 1):
                try:
                    # Find match
                    match_result = self.find_best_match(row)
                    
                    # Prepare output row
                    output_row = {
                        'property_uuid': row['property_uuid'],
                        'property_name': row['property_name'],
                        'country': row['country'],
                        'city': row['city'],
                        'email': row['email'],
                        'booking_url': row['booking_url'],
                        'match_found': match_result.get('match_found', False),
                        'match_type': match_result.get('match_type', ''),
                        'deals_checked': match_result.get('deals_checked', 0),
                        'best_score': match_result.get('best_score', 0),
                        'name_score': match_result.get('name_score', ''),
                        'signals': match_result.get('signals', ''),
                        'contact_id': match_result.get('contact_id', ''),
                        'contact_name': match_result.get('contact_name', ''),
                        'deal_id': match_result.get('deal_id', ''),
                        'deal_name': match_result.get('deal_name', ''),
                        'deal_stage': match_result.get('deal_stage', ''),
                        'location_match': match_result.get('location_match', ''),
                        'location_details': match_result.get('location_details', ''),
                        'comment': match_result.get('comment', '')
                    }
                    
                    writer.writerow(output_row)
                    
                    # Update counters
                    self.processed_count = idx
                    if match_result.get('match_found'):
                        self.matched_count += 1
                    else:
                        # Track leads without matches for human verification
                        no_match_leads.append({**row, **output_row})
                    
                    # Log progress
                    if idx % log_every == 0:
                        elapsed = (datetime.now() - self.start_time).total_seconds()
                        rate = idx / elapsed if elapsed > 0 else 0
                        eta_seconds = (total_rows - idx) / rate if rate > 0 else 0
                        eta_minutes = eta_seconds / 60
                        
                        print(f"[{idx}/{total_rows}] Processed: {idx} | "
                              f"Matched: {self.matched_count} | "
                              f"Rate: {rate:.1f} leads/s | "
                              f"ETA: {eta_minutes:.1f} min")
                
                except Exception as e:
                    print(f"  [Error] Failed to process row {idx}: {e}")
                    # Write error row
                    output_row = {
                        'property_uuid': row['property_uuid'],
                        'property_name': row['property_name'],
                        'country': row['country'],
                        'city': row['city'],
                        'email': row['email'],
                        'booking_url': row['booking_url'],
                        'match_found': False,
                        'match_type': '',
                        'deals_checked': 0,
                        'best_score': 0,
                        'name_score': '',
                        'signals': '',
                        'contact_id': '',
                        'contact_name': '',
                        'deal_id': '',
                        'deal_name': '',
                        'deal_stage': '',
                        'location_match': '',
                        'location_details': '',
                        'comment': f'ERROR: {str(e)}'
                    }
                    writer.writerow(output_row)
                    no_match_leads.append({**row, **output_row})
        
        # Final summary
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print("-" * 80)
        print(f"COMPLETED!")
        print(f"Total processed: {self.processed_count}")
        print(f"Matches found: {self.matched_count} ({self.matched_count/self.processed_count*100:.1f}%)")
        print(f"Time elapsed: {elapsed/60:.1f} minutes")
        print(f"Output saved to: {output_file}")
        
        # Export 20 random leads without matches for human verification
        if no_match_leads:
            sample_size = min(20, len(no_match_leads))
            random_sample = random.sample(no_match_leads, sample_size)
            
            human_check_file = output_file.replace('.csv', '_HUMAN_CHECK_20_RANDOM.csv')
            with open(human_check_file, 'w', newline='', encoding='utf-8') as f_check:
                writer = csv.DictWriter(f_check, fieldnames=output_fieldnames)
                writer.writeheader()
                for lead in random_sample:
                    writer.writerow(lead)
            
            print(f"Human verification file: {human_check_file} ({sample_size} random leads without matches)")
        else:
            print("No leads without matches - all leads matched!")

def main():
    input_file = "Supabase Snippet Active Leads with Valid Email.csv"
    output_file = f"hubspot_match_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Test with first 500 leads
    test_limit = 500
    
    checker = BatchHubSpotChecker()
    checker.process_csv(input_file, output_file, log_every=50, limit=test_limit)

if __name__ == "__main__":
    main()

