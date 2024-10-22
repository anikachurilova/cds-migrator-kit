# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""
import logging
import os
import json

from invenio_db import db
from invenio_rdm_migrator.load.base import Load

from cds_rdm.models import CDSMigrationAffiliationMapping

from .log import AffiliationsLogger

logger = AffiliationsLogger.get_logger()


class CDSAffiliationsLoad(Load):
    """CDSAffiliationsLoad."""

    def __init__(
        self,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run

    def _prepare(self, entry):
        """Prepare the record."""
        pass

    def _save_affiliation(self, legacy_recid, affiliations):
        """."""

        for affiliation in affiliations:
            _affiliation_model = None
            _original_input = affiliation.pop("original_input")
            if affiliation.get("matched_id"):
                _affiliation_model = CDSMigrationAffiliationMapping(
                    legacy_recid=legacy_recid,
                    legacy_affiliation_input=_original_input,
                    ror_exact_match=affiliation,
                )
            else:
                _affiliation_model = CDSMigrationAffiliationMapping(
                    legacy_recid=legacy_recid,
                    legacy_affiliation_input=_original_input,
                    ror_suggested_match=affiliation.get("ror_suggestions"),
                )
            db.session.add(_affiliation_model)
            db.session.commit()

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            legacy_recid = entry["recid"]
            creators_affiliations = entry["creators_affiliations"]
            contributors_affiliations = entry["contributors_affiliations"]
            try:
                self._save_affiliation(legacy_recid, creators_affiliations)
                self._save_affiliation(legacy_recid, contributors_affiliations)
            except Exception as ex:
                logger.error(ex)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
