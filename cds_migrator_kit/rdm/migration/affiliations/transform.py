# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import csv
import datetime
import json
import logging
import os.path
import re
import requests
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path

import arrow
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)
from opensearchpy import RequestError
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.rdm.migration.transform import affiliations_migrator_marc21
from cds_migrator_kit.rdm.migration.transform.xml_processing.dumper import CDSRecordDump

cli_logger = logging.getLogger("migrator")


class CDSToRDMAffiliationTransform(RDMRecordTransform):
    """CDSToRDMAffiliationTransform."""

    def __init__(
        self,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run
        super().__init__()

    def affiliations_search(self, affiliation_name):

        def get_ror_affiliation(affiliation):
            """Query ROR organizations API to normalize affiliations."""
            assert affiliation

            url = "https://api.ror.org/organizations"
            params = {"affiliation": affiliation}

            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                items = response.json().get("items")
                if items:
                    for item in items:
                        if item["chosen"] is True:
                            return (True, item)
                return (False, items)
            except requests.exceptions.HTTPError as http_err:
                cli_logger.exception(http_err)
            except Exception as err:
                cli_logger.exception(http_err)

        (chosen, affiliation) = get_ror_affiliation(affiliation_name)

        return (chosen, affiliation)

    def _affiliations(self, json_entry, key):
        _creators = deepcopy(json_entry.get(key, []))
        _creators = list(filter(lambda x: x is not None, _creators))
        _affiliations = []

        for creator in _creators:
            affiliations = creator.get("affiliations", [])

            for affiliation_name in affiliations:
                if not affiliation_name:
                    continue
                (chosen, match_or_suggestions) = self.affiliations_search(
                    affiliation_name
                )
                if chosen:
                    _affiliations.append(
                        {
                            "original_input": affiliation_name,
                            "matched_name": match_or_suggestions["organization"][
                                "name"
                            ],
                            "matched_id": match_or_suggestions["organization"]["id"],
                        }
                    )
                else:
                    _affiliations.append(
                        {
                            "original_input": affiliation_name,
                            "ror_suggestions": match_or_suggestions,
                        }
                    )

        return _affiliations

    def _transform(self, entry):
        """Transform a single entry."""
        # creates the output structure for load step
        record_dump = CDSRecordDump(entry, dojson_model=affiliations_migrator_marc21)
        record_dump.prepare_revisions()

        timestamp, json_data = record_dump.latest_revision
        try:
            return {
                "creators_affiliations": self._affiliations(json_data, "creators"),
                "contributors_affiliations": self._affiliations(
                    json_data, "contributors"
                ),
            }
        except Exception as e:
            cli_logger.exception(e)

    def _draft(self, entry):
        return None

    def _parent(self, entry):
        return None

    def _record(self, entry):
        return None
