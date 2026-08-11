import pathlib
import re


def sanitize_env_example():
    path = pathlib.Path("dbt/.env.example")
    if not path.exists():
        return

    replacements = {
        "DBT_SNOWFLAKE_ACCOUNT=": "DBT_SNOWFLAKE_ACCOUNT=\n",
        "DBT_SNOWFLAKE_USER=": "DBT_SNOWFLAKE_USER=\n",
        "DBT_SNOWFLAKE_PASSWORD=": "DBT_SNOWFLAKE_PASSWORD=\n",
        "DBT_SNOWFLAKE_ROLE=": "DBT_SNOWFLAKE_ROLE=\n",
        "DBT_SNOWFLAKE_DATABASE=": "DBT_SNOWFLAKE_DATABASE=\n",
        "DBT_SNOWFLAKE_WAREHOUSE=": "DBT_SNOWFLAKE_WAREHOUSE=\n",
        "DBT_SNOWFLAKE_SCHEMA=": "DBT_SNOWFLAKE_SCHEMA=\n",
        "AWS_ACCESS_KEY_ID=": "AWS_ACCESS_KEY_ID=\n",
        "AWS_SECRET_ACCESS_KEY=": "AWS_SECRET_ACCESS_KEY=\n",
        "AWS_SESSION_TOKEN=": "AWS_SESSION_TOKEN=\n",
        "TOKEN_SNOWFLAKE=": "TOKEN_SNOWFLAKE=\n",
    }

    lines = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.rstrip("\n")
            prefix = next((key for key in replacements if stripped.startswith(key)), None)
            if prefix is not None:
                lines.append(replacements[prefix])
            else:
                lines.append(line)

    path.write_text("".join(lines), encoding="utf-8")


def sanitize_snowflake_readme():
    path = pathlib.Path("snowflake/README.md")
    if not path.exists():
        return

    new_lines = []
    with path.open("r", encoding="utf-8") as f:
        skip_token_html = False
        for line in f:
            if line.strip().startswith("Tokem:"):
                new_lines.append("Tokem: <REDACTED>\n")
                continue
            if "eyJraWQiOi" in line and line.strip().startswith("```") is False:
                continue
            new_lines.append(line)

    path.write_text("".join(new_lines), encoding="utf-8")


def sanitize_setup_sql():
    path = pathlib.Path("snowflake/setup_snowflake_xeno.sql")
    if not path.exists():
        return

    text = path.read_text("utf-8")
    text = re.sub(r"AWS_KEY_ID\s*=\s*'[^']*'", "AWS_KEY_ID = '<AWS_KEY_ID>'", text)
    text = re.sub(r"AWS_SECRET_KEY\s*=\s*'[^']*'", "AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'", text)
    text = re.sub(r"AWS_TOKEN\s*=\s*'[^']*'", "AWS_TOKEN = '<AWS_SESSION_TOKEN>'", text)
    path.write_text(text, encoding="utf-8")


if __name__ == '__main__':
    sanitize_env_example()
    sanitize_snowflake_readme()
    sanitize_setup_sql()
