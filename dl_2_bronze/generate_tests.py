import yaml


def generate_tests(yml_file="tables.yml"):
    with open(yml_file) as f:
        config = yaml.safe_load(f)

    for check in config["hash_checks"]:
        filename = f"{check['name']}.sql"

        tables = check["tables"]

        with open(filename, "w") as f:
            f.write(f"-- {check['name']}\n")
            f.write("WITH hashes AS (\n\n")

            for i, t in enumerate(tables):
                table = t["table"]
                cols = ", ".join(t["columns"])

                table_name = table.split(".")[-1]
                alias = table_name

                if i == 0:
                    where = "WHERE is_current = 1"
                    from_clause = f"{{{{ source('DATAVERSE_CRM365','{table_name}') }}}}"
                else:
                    where = ""
                    from_clause = f"{{{{ ref('{table_name}') }}}}"

                f.write(f"""    SELECT
        '{alias}' AS source,
        HASH_AGG({cols}) AS hash_val
    FROM {from_clause}
    {where}
""")

                if i < len(tables) - 1:
                    f.write("\n    UNION ALL\n\n")

            f.write("\n),\n\nresult_cte AS (\n\nSELECT\n")

            for i, t in enumerate(tables):
                alias = t["table"].split(".")[-1]

                f.write(f"    MAX(CASE WHEN source = '{alias}' THEN hash_val END) AS {alias}_hash")
                if i < len(tables) - 1:
                    f.write(",\n")
                else:
                    f.write(",\n\n")

            f.write("""    CASE
        WHEN COUNT(DISTINCT hash_val) = 1 THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS result

FROM hashes

)

SELECT *
FROM result_cte
WHERE result != 'MATCH'
""")


if __name__ == "__main__":
    generate_tests()
