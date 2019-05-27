# Azure Databricks
Extension of Microsoft Databricks for Azure Data Factory examples for text processing. This project takes minimal tabular data, calls various Cognitive services, and runs from a data pipeline.

This code accompanies a public session at Azure Saturday 2019, From Crawl to Consume, A Consumer App story with Data Factory, Cognitive Services, and Search:
https://azuresaturday.de/sessions/

Source materials on Databricks from within Data Factory:
https://docs.microsoft.com/en-us/azure/data-factory/transform-data-using-databricks-notebook

There are two python notebook formats here which should contain the same code.
In the databricks environment you will need to add the following PyPI libraries to your cluster:
azure.storage.blob
bs4

Have fun!
