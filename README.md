# HubSpot Duplicate Checker - Render Deployment

Simple, clean deployment package for running HubSpot duplicate checks on Render.

## Files

- `run_duplicate_check.py` - Main script that does everything
- `requirements.txt` - Python dependencies
- `README.md` - This file

## Render Setup

### 1. Create New Cron Job
- Go to Render Dashboard
- Click "New +" → "Cron Job"
- Connect to this repository

### 2. Configuration
```
Name: hubspot-duplicate-checker
Language: Python 3
Branch: main
Build Command: pip install -r requirements.txt
Command: python3 run_duplicate_check.py
Schedule: 0 */6 * * * (every 6 hours)
```

### 3. Environment Variables
Add these in Render dashboard:
```
SUPABASE_URL=https://gnctmvssbgbqralyuewh.supabase.co
SUPABASE_API_KEY=your_supabase_key
HUBSPOT_TOKEN=your_hubspot_token
AIRTABLE_TOKEN=your_airtable_token
```

## What It Does

1. **Fetches unprocessed leads** from Supabase
2. **Checks for duplicates** in HubSpot (contacts & deals)
3. **Checks AlohaCamp** (Airtable) for existing properties
4. **Updates Supabase** with results
5. **Handles rate limiting** automatically
6. **Logs everything** for monitoring

## Configuration

The script processes **500 leads per batch** and runs **2 batches** by default (1000 leads total per run).

To modify, edit these variables in `run_duplicate_check.py`:
```python
self.batch_size = 500      # Leads per batch
self.max_batches = 2       # Number of batches
```

## Monitoring

Check the Render logs to see:
- Processing progress
- Duplicate detection results
- API rate limiting
- Error handling
- Final summary statistics

## Success Criteria

- **Exit code 0**: All leads processed successfully
- **Exit code 1**: Some leads processed with errors
- **Exit code 2**: Fatal error, no processing completed
