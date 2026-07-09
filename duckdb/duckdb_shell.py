import argparse
import os
import sys

import duckdb


def configure_s3(conn):
    s3_enabled = any(
        os.getenv(name)
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_S3_ENDPOINT")
    )
    if not s3_enabled:
        return

    conn.execute("install httpfs")
    conn.execute("load httpfs")

    settings = {
        "s3_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "s3_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "s3_region": os.getenv("AWS_DEFAULT_REGION"),
        "s3_endpoint": os.getenv("AWS_S3_ENDPOINT"),
        "s3_use_ssl": os.getenv("AWS_S3_USE_SSL"),
    }

    for key, value in settings.items():
        if value:
            escaped = value.replace("'", "''")
            conn.execute(f"set {key}='{escaped}'")


def execute(conn, sql):
    result = conn.execute(sql)
    if result.description:
        print(result.fetchdf().to_string(index=False))


def repl(conn, path):
    print(f"DuckDB: {path}")
    print("Use .exit para sair. Termine comandos SQL com ponto e virgula.")
    buffer = []

    while True:
        try:
            line = input("duckdb> " if not buffer else "   ...> ")
        except EOFError:
            print()
            break

        if line.strip() in {".exit", ".quit"}:
            break

        buffer.append(line)
        if line.rstrip().endswith(";"):
            sql = "\n".join(buffer).strip().rstrip(";")
            buffer.clear()
            try:
                execute(conn, sql)
            except Exception as exc:
                print(f"Erro: {exc}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("database", nargs="?", default=os.getenv("DUCKDB_PATH", "/data/analytics.duckdb"))
    parser.add_argument("-c", "--command")
    parser.add_argument("-f", "--file")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.database), exist_ok=True)

    conn = duckdb.connect(args.database)
    configure_s3(conn)

    if args.command:
        execute(conn, args.command)
        return

    if args.file:
        with open(args.file, "r", encoding="utf-8") as file:
            execute(conn, file.read())
        return

    repl(conn, args.database)


if __name__ == "__main__":
    main()
