import csv
import itertools
import os
import sqlite3
import sys
from typing import TYPE_CHECKING, Any, Iterable, TextIO, Union

import click
import dedupe
from dedupe._typing import (ArrayLinks, BlocksInt, DataInt, LookupResultsInt,
                            Record)

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable
else:
    from importlib.resources import resources_abc

    Traversable = resources_abc.Traversable

PathLike = Union[str, Traversable, os.PathLike]


class EstablishmentGazetteer(dedupe.StaticGazetteer):
    def __init__(
        self,
        db_path: PathLike,
        canonical_table_name: str,
        entity_table_name: str,
        settings_path: PathLike,
        num_cores=None,
    ):
        with open(settings_path, "rb") as settings_file:
            dedupe.api.StaticMatching.__init__(self, settings_file, num_cores)

        self.db: PathLike = db_path
        self.data_table_name = canonical_table_name
        self.entity_table_name = entity_table_name

        self.primary_fields = tuple(
            {var.field for var in self.data_model.primary_variables}
        )

        # Check if indexed_records table exists and create it not.
        con = sqlite3.connect(self.db)
        (indexed_records_exists,) = con.execute(
            """
            SELECT
                EXISTS (
                    SELECT
                        name
                    FROM
                        sqlite_master
                    WHERE
                        TYPE = 'table'
                        AND name = 'indexed_records')
            """
        ).fetchone()

        if not indexed_records_exists:
            self.reblock_canonical()

    def blocks(self, data: DataInt) -> BlocksInt:  # type: ignore[override]
        """
        Yield groups of pairs of records that share fingerprints.
        Each group contains one record from data_1 paired with the records
        from the indexed records that data_1 shares a fingerprint with.
        Each pair within and among blocks will occur at most once. If
        you override this method, you need to take care to ensure that
        this remains true, as downstream methods, particularly
        :func:`many_to_n`, assumes that every pair of records is compared no
        more than once.
        Args:
            data: Dictionary of records, where the keys are record_ids
                  and the values are dictionaries with the keys being
                  field names
        Examples:
            >>> blocks = matcher.pairs(data)
            >>> print(list(blocks)
            [
                [
                    (
                        (1, {"name": "Pat", "address": "123 Main"}),
                        (8, {"name": "Pat", "address": "123 Main"}),
                    ),
                    (
                        (1, {"name": "Pat", "address": "123 Main"}),
                        (9, {"name": "Sam", "address": "123 Main"}),
                    ),
                ],
                [
                    (
                        (2, {"name": "Sam", "address": "2600 State"}),
                        (5, {"name": "Pam", "address": "2600 Stat"}),
                    ),
                    (
                        (2, {"name": "Sam", "address": "123 State"}),
                        (7, {"name": "Sammy", "address": "123 Main"}),
                    ),
                ],
            ]
        """

        con = sqlite3.connect(self.db, check_same_thread=False)
        con.row_factory = sqlite3.Row

        con.execute("BEGIN")

        con.execute(
            """
            CREATE TEMPORARY TABLE blocking_map (
                block_key text,
                record_id integer)
            """
        )
        con.executemany(
            """
            INSERT INTO blocking_map
                VALUES (?, ?)
            """,
            self.fingerprinter(data.items()),
        )

        pairs = con.execute(
            """
            SELECT DISTINCT
                a.record_id AS record_id_a,
                b.record_id AS record_id_b,
                {columns}
            FROM
                blocking_map a
                INNER JOIN indexed_records b USING (block_key)
                INNER JOIN {data_table} AS dt ON b.record_id = id
            ORDER BY
                a.record_id
            """.format(
                data_table=self.data_table_name, columns=", ".join(self.primary_fields)
            )
        )

        pair_blocks: Iterable[tuple[int, Iterable[sqlite3.Row]]] = itertools.groupby(
            pairs, lambda x: x["record_id_a"]
        )

        for a_record_id, pair_block in pair_blocks:
            a_record = data[a_record_id]

            yield [
                (
                    (a_record_id, a_record),
                    (row["record_id_b"], dict(row)),
                )
                for row in pair_block
            ]

        pairs.close()
        con.execute("ROLLBACK")
        con.close()

    def search(  # type: ignore[override]
        self,
        data: DataInt,
        threshold: float = 0.0,
        n_matches: int = 1,
        generator: bool = False,
    ) -> LookupResultsInt:
        """
        Identifies pairs of records that could refer to the same entity,
        returns tuples containing tuples of possible matches, with a
        confidence score for each match. The record_ids within each
        tuple should refer to potential matches from a messy data
        record to canonical records. The confidence score is the
        estimated probability that the records refer to the same
        entity.
        Args:
            data: a dictionary of records from a messy
                  dataset, where the keys are record_ids and
                  the values are dictionaries with the keys
                  being field names.
            threshold: a number between 0 and 1. We will consider
                       records as potential duplicates if the predicted
                       probability of being a duplicate is
                       above the threshold.
                       Lowering the number will increase
                       recall, raising it will increase
                       precision
            n_matches: the maximum number of possible matches from
                       canonical_data to return for each record in
                       data. If set to `None` all possible
                       matches above the threshold will be
                       returned.
            generator: when `True`, match will generate a sequence of
                       possible matches, instead of a list.
        Examples:
            >>> matches = gazetteer.search(data, threshold=0.5, n_matches=2)
            >>> print(matches)
            [
                (((1, 6), 0.72), ((1, 8), 0.6)),
                (((2, 7), 0.72),),
                (((3, 6), 0.72), ((3, 8), 0.65)),
                (((4, 6), 0.96), ((4, 5), 0.63)),
            ]
        """
        blocks = self.blocks(data)
        pair_scores = self.score(blocks)
        search_results = self.many_to_n(pair_scores, threshold, n_matches)

        results = self._hydrate_matches(data, search_results)

        yield from results

    def _hydrate_matches(self, data: DataInt, results: ArrayLinks) -> LookupResultsInt:

        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row

        seen_messy = set()

        for result in results:

            a = None

            canonical_ids = tuple(
                str(identifier) for identifier in result["pairs"][:, 1]
            )

            entity_relations = con.execute(
                """
                SELECT
                    entity_id,
                    dt.*
                FROM
                    {entity_map}
                    INNER JOIN {data_table} dt USING (id)
                WHERE
                    id IN ({ids})
                """.format(
                    entity_map=self.entity_table_name,
                    data_table=self.data_table_name,
                    ids=", ".join(canonical_ids),
                )
            )

            entity_lookup = {row["id"]: dict(row) for row in entity_relations}

            seen_entities = set()

            prepared_result = []

            for (a, b), score in result:
                canonical = entity_lookup[b]
                entity_id = canonical.pop("entity_id")
                if entity_id not in seen_entities:
                    prepared_result.append((entity_id, b, canonical, score))
                    seen_entities.add(entity_id)

            yield ((a, data[a]), tuple(prepared_result))

            seen_messy.add(a)

        for k in data.keys() - seen_messy:
            yield (k, data[k], ())

    def __del__(self) -> None:
        pass

    def reblock_canonical(self) -> None:

        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row

        blocked_fields = tuple(
            {
                simplepred.field
                for predicate in self.predicates
                for simplepred in predicate
            }
        )

        results = (
            (row["id"], row)
            for row in con.execute(
                """
                SELECT
                    id,
                    {fields}
                FROM
                    {data_table}
                """.format(
                    fields=", ".join(blocked_fields), data_table=self.data_table_name
                )
            )
        )

        self.block_index(results, rebuild=True)

    def block_index(
        self, data: Iterable[Record], rebuild: bool = False
    ) -> None:  # pragma: no cover
        """
        Add records to the index of records to match against. If a record in
        `canonical_data` has the same key as a previously indexed record, the
        old record will be replaced.
        Args:
            data: a dictionary of records where the keys
                  are record_ids and the values are
                  dictionaries with the keys being
                  field_names
        """

        con = sqlite3.connect(self.db)

        if rebuild:
            con.execute("DROP TABLE IF EXISTS indexed_records")

        # Set journal mode to WAL.
        con.execute("pragma journal_mode=wal")

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS indexed_records (
                block_key text,
                record_id integer,
                UNIQUE (block_key, record_id))
            """
        )

        con.executemany(
            """
            REPLACE INTO indexed_records
            VALUES (?, ?)
            """,
            self.fingerprinter(data, target=True),
        )

        con.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS
                       indexed_records_block_key_idx
                       ON indexed_records
                       (block_key, record_id)"""
        )
        con.execute("""ANALYZE""")

        con.commit()
        con.close()


def preProcess(column: str) -> Union[str, None]:
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    column = column.lower()
    # If data is missing, indicate that by setting the value to `None`
    if not column:
        return None
    return column


def readData(
    f: TextIO, identifier: str, chunk_size: int = 5
) -> dict[int, dict[str, Any]]:
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    reader = csv.DictReader(f)
    for row in reader:
        clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
        row_id = int(row[identifier])
        data_d[row_id] = dict(clean_row)

        if len(data_d) == chunk_size:
            yield data_d
            data_d = {}


@click.command()
@click.argument("infile", type=click.File("r"), nargs=1, default=sys.stdin)
@click.argument("outfile", type=click.File("w"), nargs=1, default=sys.stdout)
@click.option("--identifier", type=str, nargs=1)
def main(infile: TextIO, outfile: TextIO, identifier: str):
    from importlib.resources import files

    import tqdm

    db_path = files("establishment").joinpath("gazetteer.db")
    settings_path = files("establishment").joinpath("learned_settings")

    gazetteer = EstablishmentGazetteer(
        db_path, "canonical", "entity_map", settings_path
    )

    writer = csv.writer(outfile)
    writer.writerow([identifier, "establishment_identifier", "confidence"])

    for messy_records_chunk in readData(infile, identifier):
        results = gazetteer.search(messy_records_chunk, n_matches=5, generator=False)

        for messy_record, matches in tqdm.tqdm(results):
            messy_record_id, *rest = messy_record
            for matched_record in matches:
                establishment_id, _, _, confidence = matched_record
                writer.writerow([messy_record_id, establishment_id, confidence])

        break


if __name__ == "__main__":
    main()
