# PostgreSQL backups

`backup-cronjob.yaml` creates a PostgreSQL custom-format dump every day at
03:30 Asia/Makassar, validates the dump, and stores it in an encrypted restic
repository in Linode Object Storage.

The restic password is required for every restore. Store a copy outside the
cluster and outside the Object Storage bucket.

## Linode Object Storage

Create a private bucket and a limited Object Storage access key with
`read_write` access to that bucket only. Do not configure an expiry lifecycle
policy for the restic objects; restic manages snapshot retention and pruning.

Copy the bucket's S3 endpoint hostname from Cloud Manager. Restic requires a
path-style repository URL:

```text
s3:https://S3_ENDPOINT_HOSTNAME/BUCKET_NAME/data-hunt-postgres
```

## Cluster configuration

Apply the non-secret configuration:

```sh
kubectl --context data-hunt apply -f k8s/backup-config.yaml
```

Create the credentials without committing them to Git:

```sh
kubectl --context data-hunt -n data-hunt create secret generic data-hunt-backup-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='ACCESS_KEY' \
  --from-literal=AWS_SECRET_ACCESS_KEY='SECRET_KEY' \
  --from-literal=RESTIC_PASSWORD='LONG_RANDOM_RESTIC_PASSWORD'
```

Apply the CronJob only after both resources exist:

```sh
kubectl --context data-hunt apply -f k8s/backup-cronjob.yaml
```

## Initial verification

Run the first backup immediately instead of waiting for the schedule:

```sh
kubectl --context data-hunt -n data-hunt create job \
  --from=cronjob/data-hunt-postgres-backup \
  data-hunt-postgres-backup-manual
kubectl --context data-hunt -n data-hunt logs \
  job/data-hunt-postgres-backup-manual \
  --all-containers --follow
```

A backup is accepted only after `restic check` succeeds. Before removing the
old deployment, restore the latest snapshot into a separate test database and
compare its schema revision and table counts with production.
