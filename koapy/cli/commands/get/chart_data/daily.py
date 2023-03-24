import click

from koapy.cli.utils.fail_with_usage import fail_with_usage
from koapy.cli.utils.verbose_option import verbose_option
from koapy.utils.logging import get_logger

logger = get_logger(__name__)


@click.command(short_help="Get daily OHLCV of stocks.")
@click.option("-c", "--code", metavar="CODE", help="Stock code to get.")
@click.option(
    "-o",
    "--output",
    metavar="FILENAME",
    type=click.Path(),
    help="Output filename for code.",
)
@click.option(
    "-f",
    "--format",
    metavar="FORMAT",
    type=click.Choice(["csv", "xls", "sqlite3"], case_sensitive=False),
    default="csv",
    help="Output format. (default: csv)",
)
@click.option(
    "-s",
    "--start-date",
    metavar="YYYY-MM-DD",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d"]),
    help="Most recent date to get. Defaults to today or yesterday if market is open.",
)
@click.option(
    "-e",
    "--end-date",
    metavar="YYYY-MM-DD",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d"]),
    help="Stops if reached, not included (optional).",
)
@click.option(
    "-p", "--port", metavar="PORT", help="Port number of grpc server (optional)."
)
@verbose_option()
def daily(
    code, output, format, start_date, end_date, port
):  # pylint: disable=redefined-builtin
    if (code, output, start_date, end_date) == (None, None, None, None):
        fail_with_usage()

    if output is None:
        output = "{}.{}".format(code, format)

    from koapy.backend.kiwoom_open_api_plus.core.KiwoomOpenApiPlusEntrypoint import (
        KiwoomOpenApiPlusEntrypoint,
    )

    with KiwoomOpenApiPlusEntrypoint(port=port) as context:
        context.EnsureConnected()
        df = context.GetDailyStockDataAsDataFrame(code, start_date, end_date)

    try:
        df['체결시간'] = pd.to_datetime(df['체결시간'], format="%Y%m%d%H%M%S")
        df['현재가'] = df['현재가'].astype(int).abs()
        df['시가'] = df['시가'].astype(int).abs()
        df['고가'] = df['고가'].astype(int).abs()
        df['저가'] = df['저가'].astype(int).abs()
    except Exception as e:
        print(df, str(e))
    
    if format == "xls":
        df.to_excel(output, index=False)
        logger.info("Saved data to file: %s", output)
    elif format == "csv":
        df.to_csv(output, index=False)
        logger.info("Saved data to file: %s", output)
    elif format == "sqlite3":
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + output)
        tablename = "A" + code
        df.to_sql(tablename, engine)
        logger.info("Saved data to file %s with tablename %s", output, tablename)
