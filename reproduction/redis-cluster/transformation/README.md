# Transformation

These scripts are responsible for transforming the raw experiment data stored in the `../../data` directory into DuckDB database files. These steps have to be performed with the **regular** Linux kernel:

```
./setup
./do-all
```

The DuckDB database files will be stored in the [../../data](../../data) directory (next to the raw experiment data).

Troubleshooting:
In case one of the analysis scripts crashes (e.g., due to an out-of-memory error caused by DuckDB), please delete all DuckDB database files (`cd ../../data && find . | grep duckdb | xargs rm`), reboot the system (`sudo reboot`) and start the `do-all` script again.


