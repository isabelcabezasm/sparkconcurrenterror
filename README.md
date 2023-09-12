# Spark Concurrent Error


This repository has been created to illustrate the next issue:

As you can see, I have exaclty the same code in a python [library installed in the cluster (wheel package)](src/sample/concurrent_error/main.py) and in a [notebook](notebooks/code-in-the-notebook1.ipynb). But it works in a different way.

When I execute both in parallel in the notebooks, I don't have any errors, but a ConcurrentAppendException appears when I execute the code in the library.

## How to reproduce the error
Sample data and delta table are in a [ADLS](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) so if you want to execute this library/notebooks you will need to connect the ADLS to Databricks following instructions: [Tutorial: Connect to Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/databricks/getting-started/connect-to-azure-storage) 

As you can see in the [notebooks that call the library](notebooks/mysampleforthebug2.ipynb) cell 3 is prepared for using your Key Vault backed secret scope in your Azure Databricks workspace.

Save [sample1.json](sample-data/sample1.json) and [sample2.json](sample-data/sample2.json) in your ADLS, so you can read them from DB, and configure variables `source_path_sample_1` and `source_path_sample_2` accordingly.

**Import** and **open** notebooks [mysampleforthebug](notebooks/mysampleforthebug.ipynb) and [mysampleforthebug2](notebooks/mysampleforthebug2.ipynb)
**Run** all the cells above cmd 5 in both notebooks.
Run cell 5 of both notebooks in parallel (at the same time) or with one or two seconds of difference.

You will have the Exception:

[img]

## More details

One of the steps of this code that is that executes the operation `merges` in a DeltaTable concurrently.

The code in the notebook doesn't fail, but in the Python library gives the error: ConcurrentAppendException.
This error is documented here:
https://learn.microsoft.com/en-us/azure/databricks/optimizations/isolation-level#concurrentappendexception
and as you can see in the code, the statement that solves the issue is implemented. It solves the issue if the code is being executed in the notebook, but not in a library in python.

