# PostgreSQL backups

`backup-cronjob.yaml` creates a PostgreSQL custom-format dump every day at
03:30 Asia/Makassar, validates the dump, calculates its SHA-256 checksum, and
uploads both files to private Linode Object Storage.

## Linode Object Storage

Create a private bucket and a limited Object Storage access key with
`read_write` access to that bucket only. The bucket must remain private.

## Cluster configuration

Apply the non-secret configuration:

```sh
kubectl --context data-hunt apply -f k8s/backup-config.yaml
```

Create the credentials without committing them to Git:

```sh
kubectl --context data-hunt -n data-hunt create secret generic data-hunt-backup-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='ACCESS_KEY' \
  --from-literal=AWS_SECRET_ACCESS_KEY='SECRET_KEY'
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

A backup is accepted only after the job validates the PostgreSQL archive and
confirms the uploaded S3 object's size. Before removing the old deployment,
download the latest dump, verify its SHA-256 checksum, restore it into a
separate test database, and compare its schema revision and table counts with
production.
