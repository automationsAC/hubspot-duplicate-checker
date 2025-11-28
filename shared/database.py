#!/usr/bin/env python3
"""
Shared Database Module
Handles all Supabase interactions for cron jobs
"""

import os
import requests
from typing import List, Dict, Optional
from datetime import datetime


class Database:
    def __init__(self):
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_ANON_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        
        self.headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        
        # Connection timeout settings
        self.request_timeout = (10, 30)  # (connect timeout, read timeout) in seconds
        
        # AlohaCamp Supabase project (separate from main project)
        self.alohacamp_supabase_url = os.environ.get('ALOHACAMP_SUPABASE_URL', 'https://ggrrekgtbfwcovllbovl.supabase.co')
        self.alohacamp_supabase_key = os.environ.get('ALOHACAMP_SUPABASE_KEY') or self.supabase_key
        
        self.alohacamp_headers = {
            "apikey": self.alohacamp_supabase_key,
            "Authorization": f"Bearer {self.alohacamp_supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
    
    def fetch_leads_for_hubspot_check(self, batch_size: int) -> List[Dict]:
        """Fetch leads that need HubSpot validation from the unified view"""
        url = f"{self.supabase_url}/rest/v1/lead_pipeline_view"
        params = {
            "select": ",".join([
                "property_uuid","host_uuid",
                "property_name","country","booking_url",
                "email","phone","first_name","last_name",
                "computed_score","skip_processing"
            ]),
            # Not yet duplicate-checked
            "duplicate_check_completed_at": "is.null",
            # Must have contact + property basics
            "email": "not.is.null",
            "property_name": "not.is.null",
            # Note: skip_processing filtered in Python to avoid PostgREST or parameter issues
            "order": "computed_score.desc.nullslast",
            "limit": str(batch_size * 2)  # Fetch extra to account for Python filtering
        }
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        leads = response.json()
        
        # Filter out leads with skip_processing = true (keep NULL and false)
        filtered_leads = [
            lead for lead in leads
            if lead.get('skip_processing') is None or lead.get('skip_processing') == False
        ]
        
        return filtered_leads[:batch_size]
    
    def fetch_leads_for_zerobounce(self, batch_size: int) -> List[Dict]:
        """Fetch leads that need ZeroBounce validation from the unified view"""
        url = f"{self.supabase_url}/rest/v1/lead_pipeline_view"
        params = {
            "select": ",".join([
                "property_uuid","host_uuid","email","country","computed_score",
                "humanfit","human_fit_skipped"
            ]),
            "duplicate_check_completed_at": "not.is.null",
            "already_in_pipeline": "eq.false",
            # humanfit IS NOT false (true or NULL or skipped)
            # AND skip_processing IS NOT true (NULL or false)
            # Combine both conditions with AND logic
            "or": "(humanfit.is.null,humanfit.eq.true,human_fit_skipped.eq.true)",
            "zerobounce_processed": "eq.false",
            "email": "not.is.null",
            # Exclude leads marked to skip processing (include NULL and false, exclude true)
            # Note: This needs to be combined with humanfit filter above
            # Since we can't have two 'or' params, we'll filter skip_processing in Python
            "order": "computed_score.desc.nullslast",
            "limit": str(batch_size * 2)  # Fetch extra to account for Python filtering
        }
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        leads = response.json()
        
        # Filter out leads with skip_processing = true (keep NULL and false)
        filtered_leads = [
            lead for lead in leads
            if lead.get('skip_processing') is None or lead.get('skip_processing') == False
        ]
        
        return filtered_leads[:batch_size]
    
    def fetch_leads_for_instantly_export(self, batch_size: int) -> List[Dict]:
        """Fetch leads ready for Instantly export from the unified view"""
        url = f"{self.supabase_url}/rest/v1/lead_pipeline_view"
        params = {
            "select": ",".join([
                "property_uuid","host_uuid",
                "email","first_name","last_name","property_name","country","phone",
                "booking_url","route","computed_score","added_to_instantly",
                "zerobounce_status","zerobounce_sub_status","domain_rules_check"
            ]),
            "duplicate_check_completed_at": "not.is.null",
            "already_in_pipeline": "eq.false",
            "exists_on_alohacamp": "eq.false",
            # humanfit IS NOT false
            "or": "(humanfit.is.null,humanfit.eq.true,human_fit_skipped.eq.true)",
            "zerobounce_processed": "eq.true",
            "zerobounce_status": "eq.valid",
            "email": "not.is.null",
            # ✅ FIX: Exclude leads already added to Instantly
            "added_to_instantly": "is.null",
            # Exclude previously blocked domains
            "domain_rules_check": "not.eq.blocked",
            # Exclude leads marked to skip processing (include NULL and false, exclude true)
            # Note: Filtered in Python since we already have 'or' param for humanfit
            "order": "computed_score.desc.nullslast",
            "limit": str(batch_size * 3)  # Fetch extra to account for Python-level filtering
        }
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        leads = response.json()
        
        # Filter out leads with skip_processing = true (keep NULL and false)
        leads = [
            lead for lead in leads
            if lead.get('skip_processing') is None or lead.get('skip_processing') == False
        ]
        
        # Python-level filtering for blocked domains (handles patterns)
        try:
            from .domain_blocking import is_domain_blocked
        except ImportError:
            try:
                from shared.domain_blocking import is_domain_blocked
            except ImportError:
                print("⚠️ Could not import domain_blocking, skipping Python-level filtering")
                return leads[:batch_size]
        
        filtered = []
        blocked_leads = []
        blocked_count = 0
        
        for lead in leads:
            email = lead.get('email', '').lower()
            is_blocked, reason = is_domain_blocked(email)
            
            if not is_blocked:
                filtered.append(lead)
            else:
                blocked_count += 1
                blocked_leads.append({
                    'lead': lead,
                    'reason': reason
                })
                print(f"🚫 Blocked domain: {email} - {reason}")
        
        if blocked_count > 0:
            print(f"🚫 Blocked {blocked_count} leads with problematic domains")
            # Mark blocked leads in database to avoid reprocessing
            self._mark_leads_as_domain_blocked(blocked_leads)
        
        # Limit to batch_size after filtering
        return filtered[:batch_size]
    
    def _mark_leads_as_domain_blocked(self, blocked_leads: List[Dict]) -> None:
        """Mark leads with blocked domains in duplicate_checks table to prevent reprocessing"""
        if not blocked_leads:
            return
        
        dc_url = f"{self.supabase_url}/rest/v1/duplicate_checks"
        now = datetime.now().isoformat()
        
        marked_count = 0
        for item in blocked_leads:
            lead = item['lead']
            reason = item['reason']
            property_uuid = lead.get('property_uuid')
            
            if not property_uuid:
                continue
            
            try:
                # Check if record exists
                find_params = {
                    "select": "uuid",
                    "property_uuid": f"eq.{property_uuid}",
                    "limit": "1"
                }
                find = requests.get(dc_url, headers=self.headers, params=find_params)
                find.raise_for_status()
                rows = find.json()
                
                payload = {
                    "domain_rules_check": "blocked",
                    "decision": f"Domain blocked: {reason}",
                    "checked_at": now
                }
                
                if rows:
                    # Update existing
                    dc_id = rows[0]['uuid']
                    r = requests.patch(f"{dc_url}?uuid=eq.{dc_id}", headers=self.headers, json=payload)
                    r.raise_for_status()
                else:
                    # Insert new (shouldn't normally happen, but handle it)
                    payload.update({
                        "property_uuid": property_uuid,
                        "already_in_pipeline": False,
                        "exists_on_alohacamp": False,
                        "fetched_at": now
                    })
                    r = requests.post(dc_url, headers=self.headers, json=payload)
                    r.raise_for_status()
                
                marked_count += 1
            except Exception as e:
                print(f"⚠️ Failed to mark lead as blocked: {property_uuid} - {e}")
        
        if marked_count > 0:
            print(f"✅ Marked {marked_count} leads as domain_blocked in database")
    
    def check_property_exists(self, property_name: str, country: str) -> tuple[bool, Optional[str]]:
        """Check if property exists in AlohaCamp Properties table (separate Supabase project)"""
        # Skip if AlohaCamp key is not set or same as main key (no access)
        if not self.alohacamp_supabase_key or self.alohacamp_supabase_key == self.supabase_key:
            return False, None
        
        url = f"{self.alohacamp_supabase_url}/rest/v1/properties"
        
        params = {
            "select": "uuid,property_name,country,is_published",
            "country": f"eq.{country}",
            "limit": "100"
        }
        
        try:
            response = requests.get(url, headers=self.alohacamp_headers, params=params)
            
            # If 401, silently skip (no access to AlohaCamp Supabase)
            if response.status_code == 401:
                return False, None
            
            response.raise_for_status()
            properties = response.json()
            
            if not properties:
                return False, None
            
            # Fuzzy match on property name
            from rapidfuzz import fuzz
            for prop in properties:
                db_name = (prop.get('property_name') or '').lower().strip()
                lead_name = property_name.lower().strip()
                
                if not db_name:
                    continue
                
                similarity = fuzz.ratio(db_name, lead_name)
                if similarity >= 85:  # 85% similarity threshold
                    return True, prop.get('uuid')
            
            return False, None
            
        except requests.exceptions.HTTPError as e:
            # Silently skip on 401 (unauthorized) - AlohaCamp Supabase not configured
            if e.response.status_code == 401:
                return False, None
            # Log other HTTP errors but don't fail
            return False, None
        except Exception as e:
            # Log other errors but don't fail
            return False, None
    
    def check_host_exists(self, email: Optional[str], phone: Optional[str]) -> tuple[bool, Optional[str]]:
        """Check if host exists in AlohaCamp Hosts table (separate Supabase project)"""
        if not email and not phone:
            return False, None
        
        # Skip if AlohaCamp key is not set or same as main key (no access)
        if not self.alohacamp_supabase_key or self.alohacamp_supabase_key == self.supabase_key:
            return False, None
        
        url = f"{self.alohacamp_supabase_url}/rest/v1/hosts"
        
        # Build query for email OR phone
        filters = []
        if email:
            filters.append(f'email.eq.{email}')
        if phone:
            filters.append(f'phone.eq.{phone}')
        
        params = {
            "select": "uuid,email,phone",
            "or": f"({','.join(filters)})",
            "limit": "1"
        }
        
        try:
            response = requests.get(url, headers=self.alohacamp_headers, params=params)
            
            # If 401, silently skip (no access to AlohaCamp Supabase)
            if response.status_code == 401:
                return False, None
            
            response.raise_for_status()
            hosts = response.json()
            
            if hosts:
                return True, hosts[0].get('uuid')
            
            return False, None
            
        except requests.exceptions.HTTPError as e:
            # Silently skip on 401 (unauthorized) - AlohaCamp Supabase not configured
            if e.response.status_code == 401:
                return False, None
            # Log other HTTP errors but don't fail
            return False, None
        except Exception as e:
            # Log other errors but don't fail
            return False, None
    
    def update_hubspot_check_result(self, property_uuid: str, host_uuid: Optional[str], result: Dict) -> bool:
        """Upsert duplicate_checks and set scalar status fields in operations_status after HubSpot check."""
        try:
            now = datetime.now().isoformat()
            
            # Upsert into duplicate_checks
            dc_url = f"{self.supabase_url}/rest/v1/duplicate_checks"
            find = requests.get(dc_url, headers=self.headers, params={
                "select": "uuid",
                "property_uuid": f"eq.{property_uuid}",
                "limit": "1"
            }, timeout=self.request_timeout)
            find.raise_for_status()
            rows = find.json()
            # Set domain_rules_check based on domain_blocked flag
            domain_rules_check = None
            if result.get('domain_blocked'):
                domain_rules_check = 'blocked'
            elif result.get('domain_rules_check'):
                domain_rules_check = result.get('domain_rules_check')
            
            dc_payload = {
                "property_uuid": property_uuid,
                "already_in_pipeline": result.get('already_in_pipeline', False),
                "exists_on_alohacamp": result.get('exists_on_alohacamp', False),
                "domain_rules_check": domain_rules_check,
                "checked_at": now,
                "fetched_at": now,
                "decision": result.get('decision_reason')
            }
            if rows:
                dc_id = rows[0]['uuid']
                r = requests.patch(f"{dc_url}?uuid=eq.{dc_id}", headers=self.headers, json=dc_payload, timeout=self.request_timeout)
                # Retry on 429 rate limit
                if r.status_code == 429:
                    print(f"⚠️ Rate limited (429) on duplicate_checks update, retrying after 5s...")
                    import time
                    time.sleep(5)
                    r = requests.patch(f"{dc_url}?uuid=eq.{dc_id}", headers=self.headers, json=dc_payload, timeout=self.request_timeout)
                r.raise_for_status()
            else:
                r = requests.post(dc_url, headers=self.headers, json=dc_payload, timeout=self.request_timeout)
                # Retry on 429 rate limit
                if r.status_code == 429:
                    print(f"⚠️ Rate limited (429) on duplicate_checks insert, retrying after 5s...")
                    import time
                    time.sleep(5)
                    r = requests.post(dc_url, headers=self.headers, json=dc_payload, timeout=self.request_timeout)
                r.raise_for_status()

            # Update operations_status with scalar fields (now with proper RLS)
            # Note: Constraint requires EITHER property_uuid OR host_uuid, not both
            # Since we always have property_uuid, use it only and set host_uuid to NULL
            try:
                os_url = f"{self.supabase_url}/rest/v1/operations_status"
                find_os = requests.get(os_url, headers=self.headers, params={
                    "select": "uuid,retry_count",
                    "property_uuid": f"eq.{property_uuid}",
                    "host_uuid": "is.null",  # Only match rows with property_uuid only
                    "limit": "1"
                }, timeout=self.request_timeout)
                find_os.raise_for_status()
                os_rows = find_os.json()
                
                os_update = {
                    "check_pipeline_finished": True,
                    "operation_completed_at": now
                }
                
                if os_rows:
                    # Update existing row
                    os_uuid = os_rows[0]['uuid']
                    r = requests.patch(f"{os_url}?uuid=eq.{os_uuid}", headers=self.headers, json=os_update, timeout=self.request_timeout)
                    # Retry on 429 rate limit
                    if r.status_code == 429:
                        print(f"⚠️ Rate limited (429) on operations_status update, retrying after 5s...")
                        import time
                        time.sleep(5)
                        r = requests.patch(f"{os_url}?uuid=eq.{os_uuid}", headers=self.headers, json=os_update, timeout=self.request_timeout)
                    r.raise_for_status()
                else:
                    # Insert new row with property_uuid only (host_uuid = NULL per constraint)
                    os_insert = {
                        "property_uuid": property_uuid,
                        "host_uuid": None,  # Must be NULL per valid_lead_reference constraint
                        **os_update
                    }
                    r = requests.post(os_url, headers=self.headers, json=os_insert, timeout=self.request_timeout)
                    # Retry on 429 rate limit
                    if r.status_code == 429:
                        print(f"⚠️ Rate limited (429) on operations_status insert, retrying after 5s...")
                        import time
                        time.sleep(5)
                        r = requests.post(os_url, headers=self.headers, json=os_insert, timeout=self.request_timeout)
                    r.raise_for_status()
            except Exception as os_error:
                # Log but don't fail the whole operation if operations_status update fails
                print(f"⚠️ WARNING: Could not update operations_status for property {property_uuid}: {os_error}")
                import traceback
                traceback.print_exc()
            
            return True
        except Exception as e:
            print(f"❌ ERROR updating duplicate check for property {property_uuid}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def update_zerobounce_result(self, property_uuid: str, host_uuid: Optional[str], email: str, result: Dict) -> bool:
        """Upsert email_validations and set scalar fields in operations_status after ZeroBounce."""
        try:
            now = datetime.now().isoformat()
            
            # Upsert into email_validations (now with unique constraint on host_uuid)
            ev_url = f"{self.supabase_url}/rest/v1/email_validations"
            
            # Check if record exists
            find_ev = requests.get(ev_url, headers=self.headers, params={
                "select": "uuid",
                "host_uuid": f"eq.{host_uuid}",
                "limit": "1"
            })
            find_ev.raise_for_status()
            ev_rows = find_ev.json()
            
            ev_payload = {
                "host_uuid": host_uuid,
                "email": email,
                "status": result.get('status', 'unknown'),
                "sub_status": result.get('sub_status', ''),
                "processed": True,
                "validated_at": now
            }
            
            if ev_rows:
                # Update existing record
                ev_uuid = ev_rows[0]['uuid']
                r = requests.patch(f"{ev_url}?uuid=eq.{ev_uuid}", headers=self.headers, json=ev_payload)
                r.raise_for_status()
            else:
                # Insert new record
                r = requests.post(ev_url, headers=self.headers, json=ev_payload)
                r.raise_for_status()

            # Update operations_status with scalar fields (now with proper RLS)
            # Note: Constraint requires EITHER property_uuid OR host_uuid, not both
            # Since we always have property_uuid, use it only and set host_uuid to NULL
            try:
                os_url = f"{self.supabase_url}/rest/v1/operations_status"
                find_os = requests.get(os_url, headers=self.headers, params={
                    "select": "uuid,retry_count",
                    "property_uuid": f"eq.{property_uuid}",
                    "host_uuid": "is.null",  # Only match rows with property_uuid only
                    "limit": "1"
                }, timeout=self.request_timeout)
                find_os.raise_for_status()
                os_rows = find_os.json()
                
                os_update = {
                    "zerobounce_check_finished": True,
                    "operation_completed_at": now
                }
                
                if os_rows:
                    # Update existing row
                    os_uuid = os_rows[0]['uuid']
                    r = requests.patch(f"{os_url}?uuid=eq.{os_uuid}", headers=self.headers, json=os_update, timeout=self.request_timeout)
                    r.raise_for_status()
                else:
                    # Insert new row with property_uuid only (host_uuid = NULL per constraint)
                    os_insert = {
                        "property_uuid": property_uuid,
                        "host_uuid": None,  # Must be NULL per valid_lead_reference constraint
                        **os_update
                    }
                    r = requests.post(os_url, headers=self.headers, json=os_insert, timeout=self.request_timeout)
                    r.raise_for_status()
            except Exception as os_error:
                # Log but don't fail the whole operation if operations_status update fails
                print(f"⚠️ WARNING: Could not update operations_status for property {property_uuid}: {os_error}")
                import traceback
                traceback.print_exc()
            
            return True
        except Exception as e:
            print(f"Error updating ZeroBounce for property {property_uuid}: {e}")
            return False
    
    def update_instantly_export_result(self, lead_ids: List[int]) -> bool:
        """Mark leads as exported to Instantly"""
        url = f"{self.supabase_url}/rest/v1/[Archived-donotuse]contacts_grid_view"
        
        update_data = {
            "added_to_instantly": True,
            "stage_13_processed_at": datetime.now().isoformat(),
            "last_status_update": datetime.now().isoformat()
        }
        
        # Update in batch using IN operator
        params = {"id": f"in.({','.join(map(str, lead_ids))})"}
        
        try:
            response = requests.patch(url, headers=self.headers, params=params, json=update_data)
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error updating leads: {e}")
            return False
    
    def increment_retry_count(self, property_uuid: str, host_uuid: Optional[str], stage: str, error: str) -> bool:
        """Log retry/error by incrementing retry_count and setting error fields in operations_status."""
        try:
            now = datetime.now().isoformat()
            max_retries = int(os.environ.get('MAX_RETRIES', 3))
            os_url = f"{self.supabase_url}/rest/v1/operations_status"
            
            # Fetch current row
            res = requests.get(os_url, headers=self.headers, params={
                "select": "uuid,retry_count",
                "property_uuid": f"eq.{property_uuid}",
                "host_uuid": f"eq.{host_uuid}" if host_uuid else "is.null",
                "limit": "1"
            })
            res.raise_for_status()
            rows = res.json()
            
            # Calculate new retry count
            current_retry = 0
            if rows:
                current_retry = int(rows[0].get('retry_count', 0) or 0)
            new_retry_count = current_retry + 1
            
            # Determine if permanently failed
            permanently_failed = new_retry_count >= max_retries
            
            os_update = {
                "retry_count": new_retry_count,
                "last_error": error[:1000],  # Truncate to prevent overflow
                "last_error_at": now,
                "permanently_failed": permanently_failed
            }
            
            if rows:
                # Update existing row
                os_uuid = rows[0]['uuid']
                r = requests.patch(f"{os_url}?uuid=eq.{os_uuid}", headers=self.headers, json=os_update)
                r.raise_for_status()
            else:
                # Insert new row with error data
                os_insert = {
                    "property_uuid": property_uuid,
                    "host_uuid": host_uuid,
                    **os_update
                }
                r = requests.post(os_url, headers=self.headers, json=os_insert)
                r.raise_for_status()
            
            return True
        except Exception as e:
            print(f"Error logging retry for property {property_uuid}: {e}")
            return False
    
    def get_daily_stats(self) -> Dict:
        """Get processing statistics for last 24 hours"""
        url_view = f"{self.supabase_url}/rest/v1/lead_pipeline_view"
        url_outreach = f"{self.supabase_url}/rest/v1/outreach_campaigns"
        
        # Count various metrics
        stats = {}
        
        # Processed by HubSpot today (duplicate_check_completed_at on view)
        params = {
            "select": "property_uuid",
            "duplicate_check_completed_at": f"gte.{datetime.now().date().isoformat()}"
        }
        response = requests.get(url_view, headers=self.headers, params=params)
        stats['hubspot_processed_today'] = len(response.json()) if response.ok else 0
        
        # Validated by ZeroBounce today
        params = {
            "select": "property_uuid",
            "zerobounce_validated_at": f"gte.{datetime.now().date().isoformat()}"
        }
        response = requests.get(url_view, headers=self.headers, params=params)
        stats['zerobounce_validated_today'] = len(response.json()) if response.ok else 0
        
        # Exported to Instantly today
        params = {
            "select": "uuid",
            "instantly_added_at": f"gte.{datetime.now().date().isoformat()}"
        }
        response = requests.get(url_outreach, headers=self.headers, params=params)
        stats['instantly_exported_today'] = len(response.json()) if response.ok else 0
        
        # Stuck leads (older than 2 days, not processed)
        params = {
            "select": "property_uuid",
            "duplicate_check_completed_at": "is.null"
        }
        response = requests.get(url_view, headers=self.headers, params=params)
        stats['stuck_leads'] = len(response.json()) if response.ok else 0
        
        # Failed leads (derive as outreach with instantly_status in failure states today)
        params = {
            "select": "uuid",
            "instantly_status": "in.(rejected,bad_request,unauthorized,forbidden,campaign_not_found,rate_limited,server_error,timeout,connection_error,exception)",
            "instantly_added_at": f"gte.{datetime.now().date().isoformat()}"
        }
        response = requests.get(url_outreach, headers=self.headers, params=params)
        stats['failed_leads'] = len(response.json()) if response.ok else 0
        
        return stats
    
    def update_leads_instantly_exported(self, leads: List[Dict]) -> bool:
        """Upsert Instantly results into outreach_campaigns with verification."""
        if not leads:
            return True
        
        success_count = 0
        failed_count = 0
        for lead in leads:
            # Required identifiers from lead_pipeline_view + Instantly API
            property_uuid = lead.get('property_uuid')
            host_uuid = lead.get('host_uuid')
            campaign_id = lead.get('instantly_campaign_id')
            status_flag = lead.get('instantly_status', 'added')
            instantly_lead_id = lead.get('instantly_lead_id')
            route = lead.get('route')

            if not property_uuid or not campaign_id:
                failed_count += 1
                print(f"⚠️ Skipping lead: missing property_uuid={property_uuid} or campaign_id={campaign_id}")
                print(f"   Lead data: {lead.get('email', 'no email')}")
                continue
            
            try:
                print(f"   🔍 Updating DB for {lead.get('email')}")
                print(f"      property_uuid={property_uuid}")
                print(f"      campaign_id={campaign_id}")
                print(f"      instantly_lead_id={instantly_lead_id}")
                print(f"      status={status_flag}")
                
                url = f"{self.supabase_url}/rest/v1/outreach_campaigns"
                # Upsert-like behavior: try to find existing row
                find_params = {
                    "select": "uuid,attempts,added_to_campaign",
                    "property_uuid": f"eq.{property_uuid}",
                    "host_uuid": f"eq.{host_uuid}" if host_uuid else "is.null",
                    "campaign_id": f"eq.{campaign_id}",
                    "limit": "1"
                }
                resp = requests.get(url, headers=self.headers, params=find_params)
                resp.raise_for_status()
                rows = resp.json()

                payload = {
                    "property_uuid": property_uuid,
                    "host_uuid": host_uuid,
                    "campaign_id": campaign_id,
                    "route": route,
                    "instantly_lead_id": instantly_lead_id,
                    "instantly_status": status_flag,
                    "instantly_added_at": datetime.now().isoformat(),
                    "added_to_campaign": status_flag in ["added", "created", "duplicate"],
                    "last_status_update": datetime.now().isoformat()
                }

                if rows:
                    # Update existing
                    row_id = rows[0]["uuid"]
                    attempts = int(rows[0].get("attempts", 0) or 0)
                    payload["attempts"] = attempts + 1
                    patch_url = f"{url}?uuid=eq.{row_id}"
                    r = requests.patch(patch_url, headers=self.headers, json=payload)
                    r.raise_for_status()
                else:
                    # Insert new
                    payload.setdefault("attempts", 1)
                    r = requests.post(url, headers=self.headers, json=payload)
                    r.raise_for_status()
                
                success_count += 1
                print(f"      ✅ Database write successful")
            except Exception as e:
                failed_count += 1
                print(f"❌ Error updating database for lead {property_uuid}: {e}")
                import traceback
                traceback.print_exc()
        
        if failed_count > 0:
            print(f"⚠️ Database update completed with {success_count} successes and {failed_count} failures.")
            return False  # Indicate partial or full failure
        return True

    # Backward compatibility for existing imports

# Provide legacy alias expected by some crons
SupabaseDB = Database

