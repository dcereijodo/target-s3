# target-s3

This is a [Singer](https://singer.io) target that puts data in an S3 bucket. Data
piped to the target must follow the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).
Currently this `target` supports no state parameter, it simply copies all injected
data to S3.

Based on the [Python Singer.io target template](https://github.com/singer-io/singer-target-template)

# Example

Provided the following configuration is passed to the target
```json
{
  "bucket_name": "do-da-airflow-files-stg",
  "buffer_size": 10
}
```
one can `target-s3` singer records like
```bash
echo '{"type": "RECORD", "stream": "json-paths", "record": {"ke1": "val1", "key2": "val2"}}' |\
target-s3
```
and, provided bucket `do-da-airflow-files-stg` exists, a new S3 object will be created
in the bucket
```json
# on json-paths/2019-04-29T12:18:57.335226
{"ke1": "val1", "key2": "val2"}
```

# Run
To install the `target-s3` utility as a system command, create and activate a
Python3 virtual environment
```bash
$ cd target-foobar
$ python3 -m venv ~/.virtualenvs/target-foobar
$ source ~/.virtualenvs/target-foobar/bin/activate
```
and install the package
```bash
$ pip install -e .
```

If you prefer not to install the package, you can still run the target with
`python3 target_s3/__init__.py`
