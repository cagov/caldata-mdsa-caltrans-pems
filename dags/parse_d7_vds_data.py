import glob
import os
import tarfile
import tempfile

import fsspec
import lxml
import pandas

# We are dealing with a nested tarfile, first create
# a single temporary directory to exract the first one,
# and a second temporary directory to hold the contents of
# the sub-tar files.
with tempfile.TemporaryDirectory() as d1:
    # TODO: Connect this with the data in S3 once the relay process is done/stabilized
    tarfile.open("main.tar").extractall(d1)
    with tempfile.TemporaryDirectory() as d2:
        # TODO: make this more general as to the specific file structure.
        # Also, ask Pingping to make the file structure a bit simpler
        # e.g., remove data/tmp.
        for fname in glob.glob(os.path.join(d1, "data/tmp/pems_data_20240205/*.tar.*")):
            t = tarfile.open(fname)
            t.extractall(d2)

        # Now that the individual tar files are all extracted into a single
        # nested directory structure, iterate over all of the files to
        # create a single dataframe.
        # TODO: make this more general to different district directory structures.
        files = list(
            glob.glob(
                os.path.join(d2, "pems_data/xfer/d7urms/processed/**/*.xml.gz"),
                recursive=True,
            )
        )
        dfs = []
        for i, path in enumerate(files):
            if i % 100:
                print(f"Processed {i} of {len(files)} files")

            # Open the file once, we want to do an initial read of the top-level
            # element to get the timestamp, followed by a pandas.read_xml to
            # get the inner contents.
            with fsspec.open(path, compression="infer") as f:
                it = lxml.etree.iterparse(source=f, events=("start",))

                try:
                    _, element = next(
                        (evt, el) for (evt, el) in it if el.tag == "traffic_sample"
                    )
                except StopIteration as e:
                    raise RuntimeError(
                        "Didn't find expected 'traffic_sample' element"
                    ) from e

                # Roll back the tape to feed it to pandas!
                f.seek(0)
                df = pandas.read_xml(f, dtype_backend="pyarrow")

                # Create a new column with the timestamp for this file.
                # TODO: verify the timestamp tzinfo, here assuming
                # America/Los Angeles. The
                df = df.assign(
                    sample_timestamp=(
                        pandas.to_datetime(
                            element.attrib["time_stamp"],
                            format="%a %b %d %H:%M:%S PST %Y",
                        ).tz_localize("America/Los_Angeles")
                    )
                )

                dfs.append(df)

        # Concatenate and save the data to parquet.
        df = pandas.concat(dfs)
        df.to_parquet("d7urms.parquet", compression="snappy", index=False)
