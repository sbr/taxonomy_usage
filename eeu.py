#!/usr/bin/python
# -*- coding: utf-8 -*-
import subprocess
import sys
import re
import os
import sqlite3
from bs4 import BeautifulSoup
import json

usage = "Usage: ./eeu.py full_path_to_sbr_au"

labelLookup = {}
datatypeLookup = {}
datatypeJSONLookup = {}
reclassificaionLookup = {
"DE13194":"baf/bafpo/bafpo1",
"DE2056":"baf/bafpo/bafpo1",
"DE2591":"baf/bafpr/bafpr1",
"DE3535":"baf/bafpr/bafpr1",
"DE5225":"baf/bafpo/bafpo4",
"DE8583":"baf/bafpr/bafpr1",
"DE9":"py/pyid/pyid",
"DE9087":"baf/bafot/bafot",
"DE9088":"baf/bafot/bafot",
"DE9089":"baf/bafot/bafot"}

agencyLookup = {
"apra":"Australian Prudential Regulation Agency",
"asic": "Australian Securities and Investments Commission",
"ato": "Australian Taxation Office",
"osract": "ACT Office of Revenue",
"osrnsw": "NSW Office of Revenue",
"osrnt": "NT Office of Revenue",
"osrqld": "QLD Office of Revenue",
"osrsa": "SA Office of Revenue",
"osrtas": "TAS Office of Revenue",
"osrvic": "VIC Office of Revenue",
"osrwa": "WA Office of Revenue",
"sprstrm": "SuperStream"
}

xbrlDataTypeMap = {
"xbrli:stringItemType": "string",
"xbrli:tokenItemType": "string",
"xbrli:decimalItemType": "float",
"xbrli:monetaryItemType": "float",
"xbrli:booleanItemType": "boolean",
"xbrli:dateItemType": "string",
"xbrli:nonNegativeIntegerItemType": "int",
"xbrli:positiveIntegerItemType": "int",
"xbrli:pureItemType": "float",
"xbrli:gDayItemType": "string",
"xbrli:gMonthItemType": "string",
"xbrli:gYearItemType": "string",
"xbrli:fractionItemType": "float",
"xbrli:dateTimeItemType": "string",
"xbrli:integerItemType": "int",
"xbrli:sharesItemType": "float",
"xbrli:floatItemType": "float",
"xbrli:timeItemType": "string"
}

# these datatypes have changed names since they were published
datataypeReplacements = {
"sbrPercentageBracketItemType": "sbrPercentageBracketCodeItemType",
"sbrFringeBenefitTypeItemType": "sbrFringeBenefitCodeItemType",
"sbrIncomeTaxAssessmentCalculationItemType": "sbrIncomeTaxAssessmentCalculationCodeItemType",
"sbrExpenseDeductionCodeItemType": "sbrExpenseOtherCodeItemType",
"sbrWithholdingPaymentIncomeTypeItemType": "sbrWithholdingPaymentIncomeTypeCodeItemType",
"sbrThinCapitalisationEntityTypeItemType": "sbrThinCapitalisationEntityTypeCodeItemType",
"sbrStatementProcessStatusItemType": "sbrStatementProcessStatusCodeItemType",
"sbrGstReportingOptionItemType": "sbrGstReportingOptionCodeItemType",
"sbrOrganisationTypeItemType": "sbrOrganisationTypeCodeItemType",
"sbrPartyTypeItemType": "sbrPartyTypeCodeItemType",
"sbrLodgmentFrequencyItemType": "sbrLodgmentFrequencyCodeItemType"
}

ignoredDataTypes = ["sbrReportTypeVariationCodeItemType"]

newElementNames = {

9: "Identifiers.EmploymentPayrollNumber.Identifier",
73: "OrganisationDetails.RegistrationStart.Date",
74: "OrganisationDetails.RegistrationEnd.Date",
136: "PaymentRecord.PaymentReference.Number",
226: "AuditorReport.FinancialStatementsAudited.Code",
246: "OrganisationDetails.OrganisationIndustry2006Extended.Code",
247: "OrganisationDetails.MainIncomeActivity.Description",
258: "Equity.TransferredFromReserves.Amount",
261: "Equity.TransferredToReserves.Amount",
412: "Assets.OtherFinancialAssets.CurrentDeferredAcquisitionCosts.Amount",
426: "Liabilities.OtherFinancialLiabilities.CurrentHeldForSale.Amount",
436: "Liabilities.OtherFinancialLiabilities.NonCurrentDesignatedAtFairValueThroughProfitOrLossUponInitialRecognition.Amount",
437: "Liabilities.OtherFinancialLiabilities.NonCurrentDesignatedAtFairValueThroughProfitOrLossClassifiedAsHeldForTrading.Amount",
438: "Liabilities.OtherFinancialLiabilities.NonCurrentDesignatedAtFairValueThroughProfitOrLoss.Amount",
439: "Liabilities.OtherFinancialLiabilities.NonCurrentDesignatedAtAmortisedCost.Amount",
455: "Income.Insurance.ReinsuranceRecoveriesOnUnexpiredRiskLiability.Amount",
471: "Assets.CashAndLiquidAssets.Flow.ExplorationAndEvaluationOutflowsClassifiedAsInvestingActivities.Amount",
492: "DirectorsDisclosures.DirectorsReport.PrincipalActivitiesAndSignificantChangesToPrincipalActivities.Text",
514: "DirectorsDisclosures.DirectorsReport.NonAuditServices.Text",
532: "BusinessDocument.BusinessGenerated.Identifier",
564: "Liabilities.BorrowingsAndInterestBearing.Text",
581: "Assets.OtherNonFinancialAssets.NonCurrentAssetsClassifiedAsHeldForSale.Amount",
616: "RegulatoryDisclosures.ContingentLiabilities.Text",
659: "GoodsAndServicesTax.ReportingMethod.Code",
699: "Remuneration.UnusedAnnualLongServiceLeavePaymentLumpSumA.Amount",
700: "Remuneration.UnusedAnnualLongServiceLeavePaymentLumpSumB.Amount",
701: "Remuneration.UnusedAnnualLongServiceLeavePaymentLumpSumD.Amount",
702: "Remuneration.UnusedAnnualLongServiceLeavePaymentLumpSumE.Amount",
704: "Remuneration.IndividualNonBusinessExemptForeignEmploymentIncome.Amount",
837: "Remuneration.WagesAndSalaries.PaymentsContractorAndConsultant.Amount",
962: "Assets.DebtSecuritiesNotEligibleCollateralReserveBankOfAustraliaOwnIssued.Amount",
965: "Assets.DebtSecuritiesNotEligibleCollateralReserveBankOfAustralia.Amount",
1205: "Assets.OtherNonFinancialAssets.LandAndBuildings.Amount",
1468: "Liabilities.Provisions.NonLendingLosses.Amount",
1526: "Equity.Capital.Amount",
1603: "Identifiers.AustralianFinancialServicesLicenceNumber.Identifier",
1619: "Equity.Shareholders.Amount",
1644: "IncomeTax.DeferredTotal.Amount",
1661: "Expense.AustralianInvestmentAllowableDeduction.Amount",
1690: "IncomeTax.PayAsYouGoWithholding.CreditForTaxWithheldFromCloselyHeldTrustTFNNotQuoted.Amount",
1721: "Expense.ReconciliationAdjustments.Amount",
1730: "Income.InternationalDealings.ForeignInvestmentFundAndForeignLifeAssurancePolicy.Amount",
1764: "Income.ReconciliationAdjustmentTotal.Amount",
1773: "Income.InternationalDealings.AttributedForeignIncomeForeignInvestmentFundOrForeignLifeAssurancePolicy.Indicator",
1774: "Income.InternationalDealings.AttributedForeignIncomeForeignTrustorControlledForeignCompanyorTransferorTrust.Indicator",
1794: "Expense.Operating.MotorVehicleTypeOrClaimMethod.Code",
1823: "Expense.RealEstateProperty.CapitalWorksDeduction.Amount",
1859: "IncomeTax.PayAsYouGoWithholding.CreditTaxWithheldUnusedAnnualOrLongServiceLeavePaymentLumpSumA.Amount",
1860: "IncomeTax.PayAsYouGoWithholding.CreditTaxWithheldUnusedAnnualOrLongServiceLeavePaymentLumpSumB.Amount",
1880: "Income.TrustDistributionLessNetCapitalGainAndForeignIncome.Amount",
1930: "BusinessDocument.GovernmentGenerated.Identifier",
1940: "SuperannuationContribution.PersonalContributions.Amount",
1965: "Insurance.RebatePremiumMembersDeathDisability.Amount",
1967: "Income.Investment.TotalInvestmentRevenue.Amount",
2048: "Assets.ManagedInvestments.Amount",
2054: "Assets.Investment.SecuritiesOther.Amount",
2056: "Liabilities.SecuredFunding.Amount",
2098: "Expense.RealEstateProperty.CapitalWorksDeductionRecouped.Amount",
2100: "Income.RealEstateProperty.RentalRelatedOther.Amount",
2119: "Tax.Losses.DeductedOwnershipTestFailContinuityTestPass.Amount",
2120: "Tax.Losses.CarriedForward.BusinessContinuityTestPassed.Amount",
2320: "Equity.TransfersToFromRetainedProfitsFromToReserves.Amount",
2579: "SuperannuationContribution.CapitalGainsTaxSmallBusinessExemption.Amount",
2580: "Income.AssessableIncomeDueToFundTaxStatusChange.Amount",
2581: "Income.NonArmsLengthOtherNet.Amount",
2583: "Income.NonArmsLengthPrivateCompanyDividendsNet.Amount",
2584: "Income.NonArmsLengthTrustDistributionsNet.Amount",
2589: "Expense.DeathBenefitLumpSumIncrease.Amount",
2590: "Expense.PremiumsDeathOrDisability.Amount",
2591: "Expense.Investment.Amount",
2592: "Expense.ManagementAndAdministration.Amount",
2593: "Expense.DeductibleOther.Code",
2594: "SuperannuationContribution.MemberTotal.Amount",
2595: "SuperannuationContribution.EmployerAssessable.Amount",
2598: "SuperannuationContribution.PersonalAssessable.Amount",
2601: "Equity.InternationalDealings.ForeignFundTransfers.Amount",
2610: "SuperannuationContribution.OtherThirdPartyContributions.Amount",
2622: "SuperannuationBenefit.Payments.Code",
2637: "SuperannuationRegulatoryInformation.MoneyProvidedToMembersWithoutConditionsMet.Indicator",
2812: "Assets.CurrentOtherThanAssetsOrDisposalGroupsClassifiedAsHeldForSaleTotal.Amount",
2819: "Assets.Intangible.ExcludingGoodwillNet.Amount",
2859: "Expense.ReversalsOfProvisionsOther.Amount",
2861: "ProfitOrLoss.ChangesInInventoriesOfFinishedGoodsAndWorkInProgress.Amount",
2867: "Expense.Interest.FinancialLiabilitiesNotAtFairValueThroughProfitOrLoss.Amount",
2870: "ProfitOrLoss.GainsOrLossesOnFinancialAssetsAtFairValueThroughProfitOrLossDesignatedAsUponInitialRecognition.Amount",
2871: "ProfitOrLoss.GainsOrLossesOnFinancialAssetsAtFairValueThroughProfitOrLossClassifiedAsHeldForTrading.Amount",
2872: "ProfitOrLoss.GainsOrLossesOnFinancialAssetsAtFairValueThroughProfitOrLoss.Amount",
2939: "Assets.CashAndLiquidAssets.Flow.OtherReceiptsSalesOfInterestsInAssociatesAndJointVenturesAccountedForAsAssociates.Amount",
2940: "Assets.CashAndLiquidAssets.Flow.OtherPaymentsToAcquireInterestsInAssociatesAndJointVenturesAccountedForAsAssociates.Amount",
2983: "Assets.CashAndLiquidAssets.Flow.AdjustmentsForUndistributedProfitsOfAssociatesAndJointVenturesAccountedForAsAssociates.Amount",
2989: "Equity.Movements.AdjustmentsToReconcileProfitOrLossTotal.Amount",
3077: "Income.Interest.FinancialAssetsNotAtFairValueThroughProfitOrLoss.Amount",
3106: "RegulatoryDisclosures.ContingentAssets.Text",
3116: "Equity.ContributedEquity.ParValuePerShare.Text",
3151: "Remuneration.SpecifiedPaymentsGross.Amount",
3210: "Income.DepreciatingAssetsAssessableIncomeFromBalancingAdjustmentEventsTotal.Amount",
3243: "Tax.Losses.TransferredContinuityTestFail.Amount",
3327: "Capital.Losses.DeductedOwnershipTestFailContinuityTestPass.Amount",
3332: "IncomeTax.PersonalServicesIncome.OneSourceBenchmark.Indicator",
3401: "Miscellaneous.ControlledForeignCompaniesTrustsInterest.Count",
3535: "Income.BalancingAdjustmentTotal.Amount",
4920: "Expense.InvestmentOther.Amount",
5225: "Equity.Movements.TransfersOtherTotal.Amount",
5272: "Expense.Insurance.OperatingExpensesTotal.Amount",
5306: "Assets.TotalExcludingDeferredAcquisitionCosts.Amount",
5414: "Equity.Movements.RetainedProfitsTransfersInFromGeneralFund.Amount",
5451: "SuperannuationRegulatoryInformation.ActuarialProjectionConsumerPriceIndex.Percent",
5494: "Assets.NonInvestmentOtherNetMarketValue.Amount",
5521: "Equity.Movements.RetainedProfitsTransfersOutToGeneralFund.Amount",
7395: "SuperannuationFundDetails.UniqueSuperannuationIdentifier.Identifier",
7473: "Assets.EquitySecuritiesPlusSoldEquityRepos.Amount",
7670: "Equity.CurrentYearEarnings.Amount",
7828: "Assets.CashAndLiquidAssets.DepositsAtCallEligible.Amount",
7830: "Assets.CashAndLiquidAssets.DebtSecuritiesEligibleCollateralReserveBankOfAustralia.Amount",
7831: "Assets.CashAndLiquidAssets.NegotiableCertificatesOfDeposit.Amount",
7833: "Assets.CashAndLiquidAssets.DepositsNotAtCallEligible.Amount",
7835: "Assets.CashAndLiquidAssets.MinimumLiquidityHoldingsOtherApproved.Amount",
7837: "Liabilities.Deposits.AtCall.Amount",
7838: "Liabilities.Deposits.NotAtCall.Amount",
7842: "FinancialRisk.CapitalAdequacy.SecuritisationCapitalisedStartUpCosts.Amount",
7915: "Liabilities.ContingentFundingObligation.Amount",
7996: "FinancialRisk.MinimumBoardApprovedMinimumLiquidityHoldingsRatio.Percent",
7997: "FinancialRisk.LowestMinimumLiquidityHoldingsRatio.Percent",
8085: "FinancialRisk.Market.ScalingFactorVaR.Number",
8098: "FinancialRisk.Market.ScalingFactorVaRInterestRatePositions.Number",
8099: "FinancialRisk.Market.ScalingFactorVaREquityPositions.Number",
8100: "FinancialRisk.Market.ScalingFactorForeignExchangeVaRPositions.Number",
8101: "FinancialRisk.Market.ScalingFactorVaRCommodityPositions.Number",
8254: "OrganisationDetails.MinimumBranchRequirement.Indicator",
8299: "Assets.Insurance.ReinsuranceOtherNotMeetDocumentationTest.Amount",
8300: "Assets.Insurance.ReinsuranceOtherMeetDocumentationTest.Amount",
8477: "Assets.DebtSecuritiesAndDeposits.Amount",
8494: "Assets.EquitySecuritiesOther.Amount",
8583: "Income.Operating.Amount",
8624: "Liabilities.Insurance.OutstandingClaimsAndPremiumsLiabilitiesNetSurplusDeficitTaxEffectAdjustment.Amount",
8639: "Assets.OtherFinancialAssets.SurplusDefinedBenefitSuperannuationFund.Amount",
8640: "Liabilities.OtherNonFinancialLiabilities.DeficitDefinedBenefitSuperannuationFund.Amount",
8641: "Assets.Insurance.ReinsuranceDoNotMeetDocumentationTestNet.Amount",
8642: "Assets.Insurance.ReinsuranceDoNotMeetGoverningLawRequirement.Amount",
8746: "Assets.ImmediateBorrowerClaims.Amount",
8747: "Liabilities.ImmediateBorrowerLiability.Amount",
8748: "Assets.Claims.Amount",
8749: "Assets.RiskTransferOutward.Amount",
8756: "Assets.DerivativeFinancialInstruments.UltimateRiskClaims.Amount",
8757: "Assets.Exposures.Guarantees.Amount",
8758: "Assets.Exposures.CreditCommitments.Amount",
8797: "Assets.TotalOtherGeneralInsurance.Amount",
8819: "Assets.RiskTransferInward.Amount",
8841: "Liabilities.Insurance.NetPremiumOther.Description",
8895: "Assets.LoansDeposits.Amount",
9087: "Miscellaneous.EarnoutArrangementEligibility.Code",
9088: "Miscellaneous.EarnoutArrangementDuration.Count",
9089: "Miscellaneous.EarnoutArrangementApplicableYear.Number",
12465: "Assets.SecuritisationProgramRepurchased.Amount",
12509: "Expense.ResearchAndDevelopment.ContractedExpenditureResearchServiceProvider.Amount",
12510: "Expense.ResearchAndDevelopment.ContractedExpenditureNonResearchServiceProvider.Amount",
12535: "Expense.Operating.ManagementExpense.Amount",
12711: "Identifiers.SuperannuationFundGeneratedEmployer.Identifier",
13188: "SuperannuationRegulatoryInformation.BenefitPaymentsMadeInAccordanceWithActuarialRecommendations.Code",
13194: "Assets.CollateralProvided.Amount",
13219: "SuperannuationRegulatoryInformation.MemberProfileMinimumAccountBalance.Amount",
13222: "SuperannuationRegulatoryInformation.MemberProfileMaximumSalary.Amount",
13227: "SuperannuationRegulatoryInformation.MemberProfileMaximumAccountBalance.Amount",
13229: "SuperannuationRegulatoryInformation.MemberProfileMinimumSalary.Amount",
13244: "Capital.Losses.CarriedForward.BusinessContinuityTestPassedNet.Amount",
13282: "Assets.EarningNetOfProvisions.Amount",
13599: "IncomeTax.Deduction.SelfEducation.Code",
14013: "IncomeTax.PaymentType.Code",
14705: "IncomeTax.PayAsYouGoInstalmentReporting.Code"
}

def getDataElementLabelsFromLabLink(path, elements):
    path = icls + path + ".labLink.xml"
    #print "Getting labels from",path
    f = open(path)
    soup = BeautifulSoup(f, 'xml')
    labels = soup.findAll("link:label")
    for label in labels:
        controlledid = label['xlink:label'].replace("lbl_","")
        if (controlledid not in elements): continue
        #print controlledid, label['xlink:role'], label.text
        role = "label"
        if (label['xlink:role'].lower().find("definition") > 0):role = "definition"
        if (label['xlink:role'].lower().find("guidance") > 0):role = "guidance"

        #label.text = label.text.replace("â€™","''")
        #label.text.decode("utf-8").replace("â€™", "''").encode("utf-8")
        labelText = label.text.encode('utf-8').replace("â€™","'").replace("â€˜","'").decode("utf-8")

        try:
            labelAsString = str(labelText)
        except:
            print "There is an invalid encoding in label for dataelement",controlledid
            print label.text
            sys.exit(1)

        c.execute("INSERT INTO labels VALUES ( ?, ?, ? )", (controlledid, role, labelText))

def getLabelsForDataElements(c):
    print "Getting labels for DataElements"
    c.execute("DROP TABLE IF EXISTS labels")
    c.execute("CREATE TABLE labels(controlledid text, labelrole text, label text)")

    fileList = []
    for row in c.execute("select distinct classification from latest_de"):
        fileList.append(str(row[0]))

    for file in fileList:
        deList = []
        for row in c.execute("select controlledid from latest_de where classification = '{0}'".format(file)):
            deList.append(str(row[0]))
        getDataElementLabelsFromLabLink(file,deList)

def camel_case_split(identifier):
    print identifier
    identifier = identifier.replace(".","")
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return " ".join([m.group(0) for m in matches])

def loadDataElementDetails(path, id):
    # 	<xsd:element name="OrganisationNameDetails.OrganisationalName.Text" substitutionGroup="xbrli:item" nillable="true" id="DE55" xbrli:periodType="duration" type="dtyp.02.00:sbrOrganisationNameItemType" block="substitution"/>
    if id in labelLookup:
        return labelLookup[id]

    path = path + ".data.xsd"
    newId = '"' + id + "\\\"\""
    cmd = "grep " + newId + " " + path

    for part in subprocess.check_output(cmd, shell=True).replace("\t",' ').replace("  "," ").split(" "):
        if part.startswith("name"):
            name = part.replace("\"","").replace("name=","")
            labelLookup[id] = name
        if part.startswith("type"):
            datatype = part.replace("\"","").replace("type=","")
            datatypeLookup[id] = datatype


def getDimensionLabel(path, id):
    if id in labelLookup:
        return labelLookup[id]

    path = path + ".data.xsd"
    newId = '"' + id + "\\\"\""
    cmd = "grep " + newId + " " + path

    for part in subprocess.check_output(cmd, shell=True).replace("\t",' ').replace("  "," ").split(" "):
        if part.startswith("name"):
            name = part.replace("\"","").replace("name=","")
            labelLookup[id] = name
            return name

def getDimensionsInReports(c):
    print "Extracting Dimension usage from", sbr_au

    c.execute("CREATE TABLE usage_dm(filename text, controlledid text, agency text, report text, label text)")
    x = subprocess.check_output("grep -r -i '#DM[0-9]\+' " + sbr_au_reports + " | grep -i private.*deflink", shell=True)
    for line in x.split('\n'):
        if line == "": continue
        dm = Dimension(line)
        c.execute("INSERT INTO usage_dm VALUES ('{0}','{1}','{2}','{3}', '{4}')".format(dm.filename, dm.controlledid, dm.agency, dm.report, dm.label))
    conn.commit()



def populateDataelementLatestVersion(de):
    c.execute("select classification from latest_de where controlledid = '{0}'".format(de.controlledid))
    existingClassification = c.fetchone()
    if(existingClassification == None):
        if(de.controlledid in reclassificaionLookup): de.classification = reclassificaionLookup[de.controlledid] + ".02.00"
        c.execute("INSERT INTO latest_de VALUES ('{0}','{1}','{2}','{3}')".format(de.classification, de.controlledid,de.label, de.datatype))
        return

    existingClassification = existingClassification[0]

    if(existingClassification == de.classification): return

    sameICLS = existingClassification[:-2] == de.classification[:-2]

    if sameICLS and (existingClassification < de.classification):
        #print "Updating version of ", de.controlledid, "from", existingClassification,"to",de.classification
        c.execute("delete from latest_de where controlledid = '{0}'".format(de.controlledid))
        c.execute("INSERT INTO latest_de VALUES ('{0}','{1}','{2}','{3}')".format(de.classification, de.controlledid, de.label, de.datatype))
        return

    if not sameICLS and de.classification[:-6] != reclassificaionLookup[de.controlledid]:

        if(de.controlledid not in reclassificaionLookup):
            print "Not sure what to do about reclasification that isn't in map!", de.controlledid, existingClassification, de.classification
            sys.exit(1)

        #print "reclassificaionLookup says",de.controlledid,"was reclassified to",reclassificaionLookup[de.controlledid]
        c.execute("delete from latest_de where controlledid = '{0}'".format(de.controlledid))
        c.execute("INSERT INTO latest_de VALUES ('{0}','{1}','{2}','{3}')".format(de.classification, de.controlledid, de.label, de.datatype))


def getDataElementsInReports(c):
    print "Extracting DataElement usage from", sbr_au
    c.execute("CREATE TABLE usage_de(classification text, controlledid text, agency text, report text, label text, datatype text)")
    c.execute("CREATE TABLE latest_de(classification text, controlledid text, label text, datatype text)")


    x = subprocess.check_output("grep -r -i '#DE[0-9]\+' " + sbr_au_reports + " | grep -i preslink", shell=True)
    for line in x.split('\n'):
        if line == "": continue
        if line.find("link:roleRef") > -1: continue
        de = DataElement(line)
        c.execute("INSERT INTO usage_de VALUES ('{0}','{1}','{2}','{3}', '{4}', '{5}')".format(de.classification, de.controlledid, de.agency, de.report, de.label, de.datatype))
        populateDataelementLatestVersion(de)
    conn.commit()


class Dimension():
    def __init__(self, line):
        self.line = line
        self.filename = ""
        self.label = ""
        self.controlledid = ""
        self.agency = ""
        self.report = ""
        self.extract()

    def extract(self):
        line = self.line.replace("\t",' ').replace("  "," ").replace("\\","/")
        lineparts = line.split(" ")

        urlparts = lineparts[0].replace(sbr_au_reports,"").split('/')
        if(len(urlparts) < 2):
            print "Couldn't extract agency name from " + self.line
            sys.exit(1)

        self.agency = urlparts[1]
        exitIfNull(self.agency, "Couldn't extract agency name from " + line)

        self.report = urlparts[-1][:-len(".defLink.xml:")]
        exitIfNull(self.report, "Couldn't extract report name from " + line)

        for p in range(0 ,len(lineparts)):
            part = lineparts[p]

            if part.startswith("xlink:href="):
                href = part
                dimensionparts = re.sub(r".*icls/", "", part).replace("\"","").split("#")
                self.filename = re.sub(r".*sbr_au_taxonomy/dims/", "", dimensionparts[0])[:-len(".data.xsd")]
                self.controlledid = dimensionparts[1]

            if part.startswith("xlink:label="):
                label = part[(part.find("\"")) + 1 :]
                self.label = label[0: label.rfind("\"")]

        exitIfNull(self.label, "Couldn't extract label from " + line)
        if self.label.find("Dimension") == -1:
            self.label = getDimensionLabel(dims + self.filename, self.controlledid)
        exitIfNull(self.filename, "Couldn't extract filename from " + line)
        exitIfNull(self.controlledid, "Couldn't extract controlledid from " + line)


class DataType():
    def __init__(self, element):
        self.element = element
        self.name = ""
        self.values = []
        self.facets = {}
        self.ignore = False
        self.base = ""
        self.extract()

    def extract(self):
        self.name = self.element['name']
        if(self.name in ignoredDataTypes):
            self.ignore = True
            return
        exitIfNull(self.name, "Couldn't get name from:\n" + str(self.element))

        for part in self.element.descendants:
            if part.name == "enumeration":
                self.values.append(part["value"])

            if part.name in ["maxLength","minLength","minInclusive","pattern","totalDigits","fractionDigits"]:
                self.facets[part.name] = str(part["value"])

            if part.name == "restriction":
                base = part["base"]
                if base in xbrlDataTypeMap:
                    self.base = xbrlDataTypeMap[base]

        #if self.values != []: print "Got enumerations: ", ','.join(self.values)
        #if self.facets != {}: print "Got faets ", str(self.facets)
        if self.values == [] and self.facets == {}: exitIfNull("", "Got no enumerations or facets from:\n" + str(self.element))
        exitIfNull(self.base, "Couldn't get base type from:\n" + str(self.element))

class DataElement():
    def __init__(self, line):
        self.line = line
        self.classification = ""
        self.label = ""
        self.controlledid = ""
        self.agency = ""
        self.report = ""
        self.datatype = ""
        self.extract()

    def __str__(self):
        return self.agency + ", " + self.report + ", " + self.classification + ", " + self.controlledid  + ", " + self.label + "\n"

    def extract(self):
        line = self.line.replace("\t",' ').replace("  "," ").replace("\\","/")
        lineparts = line.split(" ")

        urlparts = lineparts[0].replace(sbr_au_reports,"").split('/')
        if(len(urlparts) < 2):
            print "Couldn't extract agency name from " + self.line
            sys.exit(1)
        self.agency = urlparts[1]
        exitIfNull(self.agency, "Couldn't extract agency name from " + line)

        self.report = urlparts[-1][:-len(".presLink.xml:")]
        exitIfNull(self.report, "Couldn't extract report name from " + line)

        href = ""
        for p in range(0 ,len(lineparts)):
            part = lineparts[p]
            if part.startswith("xlink:href="):
                href = part
                dataelementparts = re.sub(r".*icls/", "", part).replace("\"","").split("#")
                self.classification = dataelementparts[0][:-len(".data.xsd")]
                self.controlledid = dataelementparts[1]

            if part.startswith("xlink:title="):
                label = part[(part.find("\"")) + 1 :]
                self.label = label[0: label.rfind("\"")]


        exitIfNull(self.classification, "Couldn't extract classification from " + line)
        exitIfNull(self.controlledid, "Couldn't extract controlledid from " + line)
        loadDataElementDetails(icls + self.classification, self.controlledid)
        self.datatype = datatypeLookup[self.controlledid]
        exitIfNull(self.datatype, "Couldn't extract datatype from " + line)

        if self.label == "" or self.label.find(".") == -1:
            self.label = labelLookup[self.controlledid]

        exitIfNull(self.label, "Couldn't extract label from " + line)


def generateDataElementJSON(c):
    print "Writing definitions to 'definitions.json'"
    dataElements = []
    for row in c.execute("select controlledid from latest_de order by controlledid"):
        dataElements.append(str(row[0]))

    elements = []
    for dataElement in dataElements:
        element = {}
        c.execute("select label from labels where controlledid = '{0}' and labelrole = 'label'".format(dataElement))
        element["name"] = c.fetchone()[0]
        if(dataElement in newElementNames):element["name"] = newElementNames[dataElement]
        try:
            c.execute("select label from labels where controlledid = '{0}' and labelrole = 'definition'".format(dataElement))
            element["definition"] = c.fetchone()[0]
        except:
            pass
        try:
            c.execute("select label from labels where controlledid = '{0}' and labelrole = 'guidance'".format(dataElement))
            element["guidance"] = c.fetchone()[0]
        except:
            pass
        element["status"] = "Standard"

        usage = []
        for agency in c.execute("select distinct agency from usage_de where controlledid = '{0}'".format(dataElement)):
            usage.append(agencyLookup[agency[0]])
        element["usage"] = usage

        justAPRA = (usage == ["Australian Prudential Regulation Agency"])
        if justAPRA:
            element["domain"] = "Financial Statistics"
            element["identifier"] = "http://dxa.gov.au/definition/fs/" + dataElement.lower()
        else:
            element["domain"] = "Standard Business Reporting"
            element["identifier"] = "http://dxa.gov.au/definition/sbr/" + dataElement.lower()

        c.execute("select datatype from latest_de where controlledid = '{0}'".format(dataElement))
        datatype = c.fetchone()[0]


        if(datatype.startswith("dtyp")):
            datatype = datatype.split(":")[1]

            if(datatype in datataypeReplacements): datatype = datataypeReplacements[datatype]

            dt = datatypeJSONLookup[datatype]
            typeDict = {"type" : dt.base}
            if(dt.values != []): typeDict["values"] = dt.values
            if(dt.facets != {}): typeDict["facets"] = dt.facets
            element["datatype"] = typeDict
        else:
            element["datatype"] = {"type" : xbrlDataTypeMap[datatype]}

        elements.append(element)



    definitions_file_name = 'definitions.json'
    if os.path.exists(definitions_file_name):
        print "Removing previous", definitions_file_name
        os.remove(definitions_file_name)
    print "Created",definitions_file_name

    text_file = open(definitions_file_name, "w")
    text_file.write(json.dumps(elements))
    text_file.close()


def getDataTypes(c):
    ## The latest datatype file should be enough
    path = fdtn + "dtyp*"
    cmd = "ls "+ path +" | sort -r | head -n 1"
    path = subprocess.check_output(cmd, shell=True).replace("\n","")

    print "Getting datatypes from",path
    f = open(path)
    soup = BeautifulSoup(f, 'xml')
    types = soup.findAll("xsd:complexType")
    for type in types:
        dt = DataType(type)
        datatypeJSONLookup[dt.name] = dt



def exitIfNull(value, message):
    if value == "" or len(value) == 0 or value == None:
        print message
        exit(1)

if len(sys.argv) != 2:
    print usage
    sys.exit(1)

sbr_au = sys.argv[1]
if (sbr_au[-1] != '/'): sbr_au = sbr_au + '/'
sbr_au_reports = sbr_au + "sbr_au_reports"
icls = sbr_au + "sbr_au_taxonomy/icls/"
fdtn = sbr_au + "sbr_au_taxonomy/fdtn/"
dims = sbr_au + "sbr_au_taxonomy/dims/"

usage_db_filename =  sbr_au.replace("/","_")[:-len("/sbr_au/")]+".db"

if os.path.exists(usage_db_filename):
    print "Removing previous database : " + usage_db_filename
    #os.remove(usage_db_filename)
print "Created usage database: '" + usage_db_filename + "'"

conn = sqlite3.connect(usage_db_filename)
c = conn.cursor()

getDataTypes(c)

#getDataElementsInReports(c)
#getLabelsForDataElements(c)
##getDimensionsInReports(c)
generateDataElementJSON(c)

conn.commit()
conn.close()
print "done."

# Domain Memebers and Values used in a report
# grep '#D[VM][0-9]\+' ctr.0007.private.02.00.defLink.xml

# Elements that are unique to an agency
# select distinct(controlledid), label from usage where agency = 'apra' and controlledid not in(select controlledid from usage where agency != 'apra') order by label

# Elemens used by an agency that are also used by others
# select distinct(controlledid), label from usage where agency = 'apra' and controlledid in(select controlledid from usage where agency != 'apra') order by label


# Datatypes used only by this agency
# select distinct(controlledid), label, datatype from usage_de where agency = 'apra' and controlledid not in(select controlledid from usage_de where agency != 'apra') and datatype not like 'xbrli%' and datatype not in (select datatype from usage_de where agency !='apra') order by label
