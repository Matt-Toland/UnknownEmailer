# UNKNOWN Brain Weekly Email System

Automated executive intelligence reports from BigQuery meeting data. Transforms qualified meeting recordings into actionable business insights with deal pipeline tracking and talent search opportunities.

## 🎯 Features

- **Rich Business Intelligence**: Extracts budgets, revenue targets, salaries, and commercial opportunities from meeting evidence
- **Automated Weekly Reports**: Generates insights from qualified meetings with NOW/NEXT/MEASURE/BLOCKER/FIT signals
- **Smart Evidence Extraction**: Quotes verbatim evidence with numbers highlighted (£2.2M revenue, £175-200k salaries)
- **Deal Pipeline Tracking**: Surfaces meetings with commercial opportunities
- **Granola Integration**: Direct links to full meeting notes
- **Clear Attribution**: Shows automated intelligence labeling and date ranges

## 🚀 Quick Start

### Send Weekly Report to SLT

```bash
curl -X POST https://brain-weekly-email-nx5wa4vtnq-nw.a.run.app/email/send \
  -H 'Content-Type: application/json' \
  -d '{"mode":"insights","to":"team@company.com"}'
```

### Preview Report

Visit: https://brain-weekly-email-nx5wa4vtnq-nw.a.run.app/email/preview/v2?mode=insights

## 📦 Installation

### Prerequisites

- Python 3.9+
- Google Cloud Project with BigQuery
- OpenAI API key (gpt-5-mini access)
- Zapier webhook for email delivery

### Local Setup

1. Clone the repository:
```bash
git clone https://github.com/Matt-Toland/UnknownEmailer.git
cd UnknownEmailer
```

2. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your credentials
```

5. Run locally:
```bash
uvicorn app.main:app --reload --port 8000
```

## 🌍 Deployment

### Deploy to Cloud Run

```bash
./deploy.sh
```

This will:
- Build Docker container
- Deploy to Cloud Run
- Set up BigQuery permissions
- Configure environment variables

### Schedule Weekly Emails

Set up Cloud Scheduler for Monday 9am delivery:

```bash
gcloud scheduler jobs create http weekly-brain-insights \
  --location=europe-west2 \
  --schedule="0 9 * * 1" \
  --time-zone="Europe/London" \
  --uri="https://YOUR-SERVICE-URL.run.app/email/send" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"mode":"insights","to":"team@company.com"}'
```

## 📊 Data Structure

The service expects BigQuery table `meeting_intel` with:
- `meeting_id`: Unique identifier
- `date`: Meeting date
- `client_info`: JSON with client details
- `now/next/measure/blocker/fit`: JSON scoring criteria
- `granola_link`: URL to full meeting notes
- `qualified`: Boolean for qualified meetings
- `total_qualified_sections`: Score (0-5)

## 🔧 Configuration

### Environment Variables

- `BQ_PROJECT_ID`: Google Cloud project ID
- `BQ_DATASET_ID`: BigQuery dataset name
- `BQ_TABLE_ID`: Meeting intelligence table
- `OPENAI_API_KEY`: OpenAI API key
- `DEFAULT_LLM_MODEL`: Model name (gpt-5-mini)
- `ZAPIER_WEBHOOK_URL`: Zapier email webhook

### Adjusting Time Period

Default is 30 days (for demo). To change to 7 days for weekly reports:

Edit `app/main.py` lines 203 and 285:
```python
days = 7  # Change from 30 to 7
```

## 📝 API Endpoints

- `GET /health` - Health check
- `GET /email/preview/v2?mode=insights` - Preview HTML email
- `POST /email/send` - Generate and send email via Zapier
- `GET /debug/data` - View raw data structure

## 🏗 Architecture

```
BigQuery (meeting_intel table)
    ↓
FastAPI Service (Python)
    ↓
OpenAI GPT-5-mini (analysis)
    ↓
HTML Email (MJML templates)
    ↓
Zapier Webhook → Email Delivery
```

## 📈 Sample Output

The system generates reports with:
- **Priority Opportunities**: Top 3 qualified meetings with evidence
- **Deal Pipeline**: All meetings with budget/revenue mentions
- **Market Intelligence**: Industry trends and pricing benchmarks
- **Hot Actions**: Immediate follow-ups needed

Example insights include:
- "Brandfuel: £2.2M ARR target, £500K profit, seeking £10M valuation"
- "UCX: Hiring Creative Director £175-200k, MD £150k, Strategy £130k"
- "Jellyfish: £80M revenue target, potential IPO within year"

## 🔐 Security

- Service account credentials not included in repo
- Cloud Run uses default service account
- All sensitive data in environment variables
- BigQuery access via IAM roles

## 📄 License

Proprietary - UNKNOWN Ltd

## 🤝 Support

For issues or questions, contact the UNKNOWN team.