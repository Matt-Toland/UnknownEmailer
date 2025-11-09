#!/bin/bash
set -e

# UNKNOWN Brain Email Service - Cloud Run Deployment Script

# Configuration
PROJECT_ID="angular-stacker-471711-k4"
SERVICE_NAME="brain-weekly-email"
REGION="europe-west2"  # London region
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "======================================"
echo "UNKNOWN Brain Email Service Deployment"
echo "======================================"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud CLI not found. Please install Google Cloud SDK."
    exit 1
fi

# Set project
echo "Setting project to ${PROJECT_ID}..."
gcloud config set project ${PROJECT_ID}

# Build container image
echo ""
echo "Building container image..."
gcloud builds submit --tag ${IMAGE_NAME}

# Deploy to Cloud Run (using default service account)
echo ""
echo "Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE_NAME} \
    --platform managed \
    --region ${REGION} \
    --port 8080 \
    --allow-unauthenticated \
    --memory 512Mi \
    --cpu 1 \
    --timeout 300 \
    --max-instances 5 \
    --set-env-vars "$(cat .env | grep -v '^#' | grep -v '^$' | tr '\n' ',' | sed 's/,$//')"

# Get the default service account used by Cloud Run
echo ""
echo "Getting Cloud Run service account..."
SERVICE_ACCOUNT=$(gcloud run services describe ${SERVICE_NAME} --region=${REGION} --format="value(spec.template.spec.serviceAccountName)")
echo "Service account: ${SERVICE_ACCOUNT}"

# Grant BigQuery permissions to the service account
echo ""
echo "Granting BigQuery permissions to service account..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/bigquery.dataViewer" \
    --condition=None 2>/dev/null || true

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/bigquery.jobUser" \
    --condition=None 2>/dev/null || true

echo ""
echo "======================================"
echo "Deployment complete!"
echo "======================================"
echo ""
echo "Service URL:"
gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format="value(status.url)"
echo ""
echo "Test endpoints:"
echo "  Health: curl \$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format='value(status.url)')/health"
echo "  Preview: curl \$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format='value(status.url)')/email/preview?mode=insights"
echo ""
