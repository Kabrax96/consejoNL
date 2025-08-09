# consejoNL


Setting up lambda function

aws ecr get-login-password --region us-east-1 \
| docker login --username AWS --password-stdin 904233088925.dkr.ecr.us-east-1.amazonaws.com

# build single-arch and push with Docker media types
docker buildx build \
  --platform linux/amd64 \
  -t 904233088925.dkr.ecr.us-east-1.amazonaws.com/consejo-etl:lambda-x86-v3 \
  --provenance=false --sbom=false \
  --output type=registry,oci-mediatypes=false \
  --push .

# (optional) verify media type
aws ecr batch-get-image \
  --repository-name consejo-etl \
  --image-ids imageTag=lambda-x86-v3 \
  --query 'images[0].imageManifestMediaType' \
  --region us-east-1

aws lambda update-function-code \
  --function-name consejo-etl-func \
  --image-uri 904233088925.dkr.ecr.us-east-1.amazonaws.com/consejo-etl:lambda-x86-v3 \
  --region us-east-1


aws lambda invoke \
  --function-name consejo-etl-func \
  --payload '{"pipeline":"egresos_single"}' \
  --cli-binary-format raw-in-base64-out \
  --region us-east-1 out.json && cat out.json

aws logs tail /aws/lambda/consejo-etl-func --since 5m --region us-east-1
