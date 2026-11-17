# Example UDF

`tshirt_sizing/` is the source of the Python UDF used by `functions/is_smaller.sql`.
`build/tshirt_sizing.zip` is its built package (an sdist wrapped in a ZIP, as Confluent requires):

```bash
cd udfs/tshirt_sizing
uv build --sdist --out-dir /tmp/tshirt_sizing_build
cd .. && mkdir -p build && python3 -c "import zipfile,glob,os; s=glob.glob('/tmp/tshirt_sizing_build/*.tar.gz')[0]; zipfile.ZipFile('build/tshirt_sizing.zip','w').write(s, os.path.basename(s))"
```

`shout/` is the source of the Java UDF used by `functions/shout.sql`: a Maven project whose
`mvn package` produces `shout.jar` (copied to `build/shout.jar`), which Confluent takes as is (no wrapping). It needs Maven and a
JDK (11 or later).

`make` in `udfs/` rebuilds both packages into `build/`, which is safe to git-ignore.
