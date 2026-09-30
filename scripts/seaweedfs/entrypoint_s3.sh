#!/bin/sh
set -e

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set for SeaweedFS S3 auth"
  exit 1
fi

mkdir -p /etc/seaweedfs

cat > /etc/seaweedfs/s3.json <<EOF
{
  "identities": [
    {
      "name": "polyhorizon-admin",
      "credentials": [
        {
          "accessKey": "${AWS_ACCESS_KEY_ID}",
          "secretKey": "${AWS_SECRET_ACCESS_KEY}"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "Write",
        "List",
        "Tagging",
        "ReadAcp",
        "WriteAcp"
      ]
    }
  ]
}
EOF

exec weed s3 -filer=filer:8888 -port=8333 -config=/etc/seaweedfs/s3.json
