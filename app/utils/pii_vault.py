import logging
from typing import Dict, List, Any, Optional

try:
    from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern, RecognizerRegistry
    from presidio_analyzer.nlp_engine import NlpEngineProvider
    from presidio_anonymizer import AnonymizerEngine
except ImportError:
    AnalyzerEngine = None
    AnonymizerEngine = None
    NlpEngineProvider = None
    PatternRecognizer = None
    Pattern = None
    RecognizerRegistry = None

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column classification sets
# ---------------------------------------------------------------------------

# UUIDs that directly identify a person (service user, staff, visitor, etc.)
_PERSON_ID_COLUMNS = {
    "ServiceUserId", "AboutMeServiceUserId", "ComplainantServiceUserId",
    "ComplainantStaffId", "ComplaintRelatedToServiceUserId", "ComplaintRelatedToStaffId",
    "ReportingServiceUserId", "ReportingStaffId", "InvolvedServiceUserId",
    "InvolvedStaffId", "InvolvedUserDetailId", "InvolvedPersonId",
    "PerpetratorServiceUserId", "PerpetratorStaffId", "PersonCausingServiceUserId",
    "PersonCausingStaffId", "PersonAtRiskId", "PersonAtRiskUserDetailId",
    "WitnessServiceUserId", "WitnessStaffId", "InterviewedServiceUserId",
    "InterviewedStaffId", "InterviewAndSupportStatementServiceUserId",
    "InterviewAndSupportStatementUserId", "InvestigationReportServiceUserId",
    "InvestigationOrInvestigatorUserId", "InvestigatorStaffUserId",
    "InverstigationReportUserDetailId", "BookedByUserDetailId",
    "ClosedBy", "ClosedByUserId", "CompletedBy", "CompletionAuthorisedByUserDetailId",
    "CreatorUserDetailId", "CreatedOrUpdatedUserId", "ModifiedBy",
    "ModifierUserDetailId", "LineManagerUserDetailId", "CheckedPersonId",
    "SeniorManagerCheckedUserId", "ReviewedBy", "PersonResponsibleId",
    "ResponsibleUserDetailId", "ProposedActionPlanByUserId",
    "SuperviseeId", "SuperviseeUserDetailId", "SupervisorId",
    "SupervisionCompletedByUserDetailId", "MasterSupervisionId",
    "UserDetailId", "UserDetailsId", "UserId", "UserDetailSupervisionId",
    "DiscussionFor", "DiscussionWith", "FamilyNotifyPersonId",
    "FinanceRecordVerifierStaffId", "FinanceRecordVerifierUserId",
    "FinanceWithdrawalManagerId", "FinanceWithdrawalPersonId",
    "FinanceRecordAgainstUnknownId", "FinanceRecordAgainstWithdrawId",
    "VisitdUserId", "VisitorId", "VisitorUserDetailId", "VisitorUserDetailId",
    "MemberOfStaffReceivingComplaintUserId", "SenderUserDetailId",
    "WhoReportWithinOrganizationStaffId", "RiskAsseOtherUserId",
    "RiskAssesmentServiceUserId", "ProcessAndReportInvestigationAuthorisedId",
    "ProcessAndReportInvestigatorId", "ProposedContractedHourUserDetailId",
    "ProposedStartDateUserDetailId", "InvestigationPlanningIdForInterviewedPerson",
    "InvestigationPlanningIdForSupplyStatement",
    "InvestigationPlanningIdInvestigationCompleted",
    "InvestigationPlanningIdInvestigator", "InvestigationProcessIdForInterviewed",
    "InvestigationProcessIdForNotInterviewed",
}

# UUIDs that identify sensitive records (incidents, audits, investigations, etc.)
_RECORD_ID_COLUMNS = {
    "ABCFormId", "AnnualCheckMonitoringId", "AnnualCheckMonitoringMasterId",
    "AuditId", "AuditMasterId", "BowelMovementChartMasterId",
    "CommunicationPassportId", "ComplaintOrConcernOrComplementId",
    "ComplaintOrConcernOrComplementReportId", "CostingCalculatorHourDetailId",
    "CostingCalculatorHourMasterId", "CostingCalculatorRatesAndPayChargesId",
    "CourseId", "CourseResultId", "DailyLogId", "DailyLogMasterId",
    "FinanceRecordId", "FinanceRecordMasterId", "FoodAndFluidChartId",
    "FoodAndFluidChartMasterId", "GroupedReferenceKey", "HospitalPassportId",
    "IncidentId", "InvestigationId", "InvestigationProcessId",
    "InvestigationProcessAndReportId", "LocationId", "MasterFieldId",
    "PositiveBehaviourSupportPlanId", "PreFormId", "PreRiskAssid",
    "PreServiceUserSupportPlanid", "PreSupervisionId",
    "RecruitmentChecklistMasterId", "ReferencePositiveBehaviourSupportPlanId",
    "ReferenceRiskAssessmentId", "ReferenceServiceUserSupportPlanId",
    "RestraintId", "RiskAssesmentId", "RiskAssessmentId", "RoutineId",
    "SafeguardingId", "SafeguardingInvestigationPlanningId",
    "SafeguardingReferenceId", "SeizureLogId", "SeizureLogMasterId",
    "ServiceUserSupportPlanId", "SingleAssessmentFrameworkId",
    "SingleAssessmentFrameworkMasterId", "SiteId",
    "SupportPlanId", "TicketId", "TenantNotificationId",
    "WebhookEventId", "WebhookSubscriptionId", "QuestionGroupId",
    "QuestionId", "QuestionOptionId", "OperatorId",
    "ParentFeedbackId", "SupervisionSupportPlanId",
}

# Decimal / financial fields
_FINANCIAL_COLUMNS = {
    "Amount", "Balance", "ExistingBalance", "WithdrawedBalance",
    "CoreHourlyCharge", "CoreHourlyPay", "CoreHours", "TotalCoreHours",
    "DayHourlyCharge", "DayHourlyPay",
    "CommunityAccessHourlyCharge", "CommunityAccessHourlyPay",
    "SleepPerNight", "SleepPerNightCharge", "SleepPerNightPay",
    "TotalSleepPerNight", "SleepPerNight", "WakingNightPerHourCharge",
    "WakingNightPerHourPay", "WakingNightPerNight", "WakingNightPerNightCharge",
    "WakingNightPerNightPay", "TotalWakingNightPerNight",
    "TotalCADurationInDecimal", "TotalDurationInDecimal", "TotalWNDurationInDecimal",
    "ManagerRating", "MarksObtain", "CommunityAccessHourlyCharge",
    "CommunityAccessHourlyPay",
}

# Dates/times that carry sensitive personal context
_SENSITIVE_DATE_COLUMNS = {
    "DateOfBirth", "DateTimeOfIncident", "TimeOfIncident", "TimeFamilyNotified",
    "DateOfAdmission", "BowelMovementDateAndTime", "LogEntryDate", "LogEntryTime",
    "FamilyReportingDate", "SafeguardingOfficeDate", "DateOfPoliceInformed",
    "DateOfCQCNotified", "DateOfComplaintReceived", "DateOfAcknowledgement",
    "DateOfDiscussion", "DateOfDiscussionWithComplainant", "DateOfInvestigation",
    "DateOfFindingInvestigation", "InvestigationBeganDate",
    "InvestigationReportDate", "InvestigationMeetingToBeCompletedByDate",
    "SupervisionDate", "SupervisionCompletedDate", "SuperviseeConfirmationDate",
    "ManagerConfirmationDate", "DateOfDBS", "CourseCompletionDate",
    "BookedDate", "VerifiedDate", "ReviewedDate", "ConfirmationDate",
    "DateRecruitmentChecklistStart", "RecruitmentChecklistCompletionDate",
}

# Keywords in column names that indicate a sensitive date even if not listed above
_SENSITIVE_DATE_KEYWORDS = {
    "birth", "incident", "admission", "police", "investigation",
    "interview", "complaint", "reporting", "notify", "notified",
    "safeguard", "dbs", "offence",
}


class PIIVaultManager:
    _instance = None

    # Regex patterns for use in free text
    _UUID_PATTERN = (
        r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}'
        r'-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b'
    )
    _AMOUNT_PATTERN = r'(?:£|\$|€)\s*\d{1,3}(?:,\d{3})*(?:\.\d{1,4})?'

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PIIVaultManager, cls).__new__(cls)
            if AnalyzerEngine is not None:
                logger.info("PIIVault: Initializing Presidio Analyzer...")
                provider = NlpEngineProvider(nlp_configuration={
                    "nlp_engine_name": "spacy",
                    "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
                })
                registry = RecognizerRegistry()
                registry.load_predefined_recognizers()

                # Custom recognizer: raw UUIDs appearing in free text / log strings
                registry.add_recognizer(PatternRecognizer(
                    supported_entity="DATABASE_ID",
                    patterns=[Pattern("UUID", cls._UUID_PATTERN, 0.95)],
                    context=["id", "user", "service", "staff", "record", "ref"],
                ))

                # Custom recognizer: monetary amounts with currency symbol in free text
                registry.add_recognizer(PatternRecognizer(
                    supported_entity="FINANCIAL_AMOUNT",
                    patterns=[Pattern("CURRENCY_AMOUNT", cls._AMOUNT_PATTERN, 0.80)],
                    context=["amount", "balance", "charge", "pay", "rate", "fee", "cost"],
                ))

                cls._instance.analyzer = AnalyzerEngine(
                    registry=registry,
                    nlp_engine=provider.create_engine(),
                )
                cls._instance.anonymizer = AnonymizerEngine()
            else:
                cls._instance.analyzer = None
                cls._instance.anonymizer = None
                logger.warning(
                    "PIIVault: presidio_analyzer or presidio_anonymizer not installed. "
                    "PII Anonymization will be a no-op."
                )
        return cls._instance

    # ------------------------------------------------------------------
    # Free-text anonymization (original method — now catches UUIDs too)
    # ------------------------------------------------------------------

    def anonymize_text(self, text: str, vault_map: Dict[str, str]) -> str:
        if not text or not self.analyzer:
            return text

        target_entities = [
            "PERSON", "LOCATION", "EMAIL_ADDRESS", "PHONE_NUMBER",
            "UK_NHS", "UK_NI", "IP_ADDRESS", "DATE_TIME", "CREDIT_CARD",
            # Custom entities added in __new__
            "DATABASE_ID", "FINANCIAL_AMOUNT",
        ]

        results = self.analyzer.analyze(
            text=text,
            entities=target_entities,
            language='en',
            score_threshold=0.35,
        )

        if not results:
            return text

        anonymized_text = text
        for res in sorted(results, key=lambda x: x.start, reverse=True):
            real_text = anonymized_text[res.start:res.end]
            key = self._get_or_create_key(res.entity_type, real_text, vault_map)
            anonymized_text = anonymized_text[:res.start] + key + anonymized_text[res.end:]

        return anonymized_text

    # ------------------------------------------------------------------
    # Structured row anonymization (new)
    # ------------------------------------------------------------------

    def anonymize_rows(
        self,
        rows: List[Dict[str, Any]],
        vault_map: Dict[str, str],
        schema: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Anonymize a list of DB row dicts.

        Pass ``schema`` as {column_name: sql_data_type} to enable
        data-type-driven fallback coverage for columns not in the
        named sets (e.g. any UNIQUEIDENTIFIER column not yet listed).
        """
        if not self.analyzer:
            return rows
        return [self._anonymize_row(row, vault_map, schema) for row in rows]

    def _anonymize_row(
        self,
        row: Dict[str, Any],
        vault_map: Dict[str, str],
        schema: Optional[Dict[str, str]],
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for col, val in row.items():
            if val is None:
                result[col] = val
                continue

            data_type = (schema or {}).get(col, "").upper()

            if col in _PERSON_ID_COLUMNS:
                # Direct person identifier — always vault
                result[col] = self._get_or_create_key("DATABASE_ID", str(val), vault_map)

            elif col in _RECORD_ID_COLUMNS:
                # Sensitive record identifier — always vault
                result[col] = self._get_or_create_key("DATABASE_ID", str(val), vault_map)

            elif col in _FINANCIAL_COLUMNS:
                result[col] = self._get_or_create_key("FINANCIAL_AMOUNT", str(val), vault_map)

            elif col in _SENSITIVE_DATE_COLUMNS:
                result[col] = self._get_or_create_key("DATE_TIME", str(val), vault_map)

            # ── Schema-driven fallbacks for columns not yet in named sets ──

            elif data_type == "UNIQUEIDENTIFIER":
                # Any UUID column not explicitly listed above
                result[col] = self._get_or_create_key("DATABASE_ID", str(val), vault_map)

            elif data_type in ("DATETIME2", "TIME", "DATE") and self._col_is_sensitive_date(col):
                # Date column whose name suggests personal context
                result[col] = self._get_or_create_key("DATE_TIME", str(val), vault_map)

            elif data_type in ("DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY") and self._col_is_financial(col):
                result[col] = self._get_or_create_key("FINANCIAL_AMOUNT", str(val), vault_map)

            elif isinstance(val, str) and len(val) > 3:
                # Free-text: run full NLP + custom-regex pipeline
                result[col] = self.anonymize_text(val, vault_map)

            else:
                result[col] = val

        return result

    # ------------------------------------------------------------------
    # De-anonymization
    # ------------------------------------------------------------------

    def deanonymize(self, text: str, vault_map: Dict[str, str]) -> str:
        if not text or not vault_map:
            return text
        deanonymized = text
        for placeholder, real_text in vault_map.items():
            deanonymized = deanonymized.replace(placeholder, real_text)
        return deanonymized

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_or_create_key(
        self, entity_type: str, real_text: str, vault_map: Dict[str, str]
    ) -> str:
        """Return existing placeholder or mint a new one.

        vault_map convention (matches original code):
            key   = placeholder  e.g. "<PERSON_1>"
            value = real text    e.g. "John Smith"
        """
        # Return placeholder if this real value is already vaulted
        for placeholder, stored in vault_map.items():
            if stored == real_text and placeholder.startswith(f"<{entity_type}"):
                return placeholder

        # Mint a new placeholder
        count = sum(1 for k in vault_map if k.startswith(f"<{entity_type}")) + 1
        placeholder = f"<{entity_type}_{count}>"
        vault_map[placeholder] = real_text
        return placeholder

    @staticmethod
    def _col_is_sensitive_date(col_name: str) -> bool:
        lower = col_name.lower()
        return any(kw in lower for kw in _SENSITIVE_DATE_KEYWORDS)

    @staticmethod
    def _col_is_financial(col_name: str) -> bool:
        financial_keywords = {
            "amount", "balance", "charge", "pay", "rate",
            "fee", "cost", "price", "salary", "wage",
        }
        lower = col_name.lower()
        return any(kw in lower for kw in financial_keywords)


pii_vault = PIIVaultManager()