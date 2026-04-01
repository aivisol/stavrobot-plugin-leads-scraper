# plugin-leads-scraper

A stavrobot plugin that generates B2B leads by running the [Apify `pipelinelabs/lead-scraper-apollo-zoominfo-lusha-ppe`](https://apify.com/pipelinelabs/lead-scraper-apollo-zoominfo-lusha-ppe) actor. It searches Apollo, ZoomInfo, and Lusha to find targeted contacts based on company and person filters.

## Setup

1. Get your Apify API token from <https://console.apify.com/account/integrations>
2. Create `config.json` in the plugin directory (next to `manifest.json`):

```json
{
  "apify_api_key": "apify_api_XXXXXXXXXXXX"
}
```

> `config.json` is listed in `.gitignore` and will never be committed.

## Tool: `scrape_leads`

Runs the lead scraper with the filters you specify and returns structured contact data.

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `company_industry_includes` | string | Comma-separated industries (e.g. `Telecommunications,Software`) |
| `company_keyword_includes` | string | Keywords in company description (e.g. `ivr,voip`) |
| `company_location_country_excludes` | string | Countries to exclude for company HQ |
| `person_location_country_includes` | string | Countries where person must be located |
| `person_location_country_excludes` | string | Countries to exclude for person |
| `person_location_state_includes` | string | US states to include |
| `person_location_state_excludes` | string | US states to exclude |
| `person_location_city_includes` | string | Cities to include |
| `person_location_city_excludes` | string | Cities to exclude |
| `person_title_includes` | string | Job titles to target (e.g. `Director,Manager,CEO,Founder`) |
| `has_email` | boolean | Only return leads with email (default: `true`) |
| `has_phone` | boolean | Only return leads with phone (default: `false`) |
| `include_similar_titles` | boolean | Include similar job titles (default: `false`) |
| `total_results` | integer | Max leads to return (default: `100`) |
| `reset_saved_progress` | boolean | Clear pagination state and start fresh (default: `false`) |

### Example Input

```json
{
  "company_industry_includes": "Telecommunications",
  "company_keyword_includes": "ivr",
  "company_location_country_excludes": "United States",
  "person_location_country_includes": "United States",
  "person_location_country_excludes": "India",
  "person_location_state_includes": "California",
  "person_location_state_excludes": "Texas",
  "person_location_city_includes": "New York",
  "person_location_city_excludes": "Dhaka",
  "person_title_includes": "Director,Manager,Founder,CEO,CTO,COO",
  "has_email": true,
  "has_phone": false,
  "include_similar_titles": false,
  "total_results": 100
}
```

### Output

```json
{
  "status": "success",
  "total_leads": 87,
  "run_id": "abc123",
  "dataset_id": "def456",
  "leads": [
    {
      "firstName": "Jane",
      "lastName": "Doe",
      "title": "Director of Operations",
      "email": "jane.doe@example.com",
      "company": "Acme Telecom",
      ...
    }
  ]
}
```

## Runtime

The tool is async (`"async": true`) because Apify actor runs can take several minutes. The script polls for completion and returns once results are ready (up to 10 minutes).
