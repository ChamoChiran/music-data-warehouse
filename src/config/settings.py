COUNTRIES = [
    "United States",
    "United Kingdom",
    "Canada",
    "Australia",
    "Germany",
    "France",
    "Japan",
    "Italy",
    "Spain",
    "Netherlands",
    "India"
]

server = r'JOLLYROGGER\SQLEXPRESS'
database = 'DataWarehouse'

CONNECTION_STRING = (
        f"mssql+pyodbc://@{server}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
    )