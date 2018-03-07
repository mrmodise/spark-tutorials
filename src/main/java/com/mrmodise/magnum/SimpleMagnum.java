package com.mrmodise.magnum;

import com.google.common.base.Splitter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleMagnum {
    public static void main(String[] args) {
        String str = ";null;:LIFE 1 POLICY 1 DECISION#;LIFE=Standard COT HIV RL2;CPI=Standard CLD COT HIV OOD RL2;DD=Standard COT HIV RL2;Evi=COT HIV;null;:LIFE 2 GENERAL#;Abroad=False;IslandSequence=medical;SanQuoteKeyPerson=UnKnown;StandardExclusionsBackAndPsych=UnKnown;RateGroupSystem=0;Decision=S ;null;null;:LIFE 1 SUM ASSURED#;ExistingThisCompanyLife=0;ExistingThisCompanyLifeExp=0;ExistingOtherCompaniesLife=0;ExistingThisCompanyWOP=0;ExistingOtherCompaniesWOP=0;ExistingThisCompanyTPD=0;ExistingOtherCompaniesTPD=0;ExistingPensionTPD=0;ExistingThisCompanyCPI=0;ExistingOtherCompaniesCPI=0;ExistingThisCompanyFIP=0;ExistingThisCompanyPYI=0;ExistingThisCompanyDD=0;ExistingOtherCompaniesDD=0;ExistingThisCompanyACC=0;ExistingThisCompanyACCInjury=0;ExistingThisCompanyGIB=0;ExistingThisCompanyPHD=0;ExistingOtherCompaniesPHD=0;ExistingThisCompanyPHI=0;ExistingOtherCompaniesPHI=0;ExistingPensionPHI=0;ExistingThisCompanyTIP=0;ExistingOtherCompaniesTIP=0;ExistingThisCompanyBOP=0;ExistingOtherCompaniesBOP=0;KeymanExisting=UnKnown;TotalLifeSumAssuredPastTwoYears=550000;Decision=S ;null;null;:LIFE 1 BUILD#;Height=163;Weight=62;WeightChangeType=Unchanged;WeightChange=0;BodyMassIndex=23.335466;Decision=S ;null;null;:LIFE 1 ALCOHOL#;AlcoholUnitsPerWeek=1;AlcoholAdvice=False;Decision=S ;null;null;:LIFE 1 TOBACCO#;CigarettesPerDay=0;SmokedPastTwelveMonths=False;Decision=S ;null;null;:LIFE 1 HABIT COMBINATION#;Decision=S ;null;null;:LIFE 1 OCCUPATION 1#;Occupation=telephone technician;Concept=telephoneengineer(noind);AdminWork=100;NonAdminManualLabour=0;NonAdminSupervision=0;NonAdminTravel=0;NonAdminWork=UnKnown;DisClass=4;AccClass=0;ClauseCodeOccupation=UnKnown;Decision=S ;null;null;:LIFE 7 OCCUPATION END#;OccupationHazardousPastFuture=False;Decision=S ;null;null;:LIFE 1 POLICY 1 OMS#;Decision=S ;null;null;:LIFE 1 POLICY 1 RATE GROUP#;RateGroupPolicy=2;RateGroupPolicy=2;null;RateGroup=2;RateGroupTop=2;RateGroupOccupationLIFE=0;Decision=S RL2;null;null;:LIFE 1 IMPAIRMENT 1#;Impairment=underactive thyroid;Concept=hypothyroidism;ProblemDate=UnKnown;FullyRecovered=False;OnTreatment=True;Decision=S ;null;null;:LIFE 1 IMPAIRMENT END#;AbsentFromWork=False;Decision=S ;null;null;:LIFE 1 OTHER MEDICAL#;OtherTreatment=False;PreviousMedicalEvidenceThisCompany=False;PreviouslyClaimed=False;SpecialInvestigation=False;Decision=S ;null;null;null;:LIFE 1 PREVIOUSLY DECLINED OR RATED#;PreviouslyDeclinedRated=False;ExistingMedicalClause=False;Decision=S ;null;null;:LIFE 1 AIDS QUESTIONNAIRE#;AidsQuestion=False;Decision=S ;null;null;:LIFE 1 POLICY 1 NON MEDICAL LIMITS#;Decision=S COT HIV;null;null;:LIFE 1 POLICY 1 FINANCIAL#;Decision=S ;null;null;:LIFE 1 POLICY 1 SPECIAL EVENTS#;LOAEntry=False;Decision=S ;null;null;null;";
        String str2 = ";null;:GLOBAL#;CompanyVersion=08.03.0053 (08/10/2007) 15:30;RunDate=20/11/2007;Environment=HOS;Language=English;Legacy=False;BlueSkyProduct=True;QuoteOnly=False;BrokerCode=00604577;null;null;:LIFE 1 OVERALL DECISION#;LIFE=Refer HIV RL1 URD;WOP=Refer CLC HIV RL1 URD;TPD=Refer CLC HIV OOD RL1 URD;Evi=HIV URD;NMLEvidence=HIV;null;:LIFE 1#;Name=DU TOIT CHRISTOFFEL F.;Surname=DU TOIT;Forenames=CHRISTOFFEL F.;Age=20;DateOfBirth=05/12/1988;AgeNextBirthday=20;Sex=Male;MaritalStatus=Single;Education=Grade 12 (Std 10);IncomeLifeAssured=25200;IncomeSpouseGuardian=0;IncomeRateGroup=25200;IncomeFinancial=25200;UnderwritingType=UnKnown;null;null;:LIFE 1 POLICY 1 ALLOWABLE COVER#;AllowableLIFEAmount=478800;AllowableDisabilityAmount=472500;null;:LIFE 1 POLICY 1 EXCEEDED COVER#;ExceededLIFEAmount=0;ExceededDisabilityAmount=0;null;:POLICY 1#;PolicyName=0425152865;PolicyType=T02W;PolicyOwner=LIFE 1;SecondOwner=False;InsurableInterest=Own life;SecurityForLoan=False;FutureCoverOption=False;SourcePlanSmokerStatus=False;Premium=145.45;ProposedLife=Life 1;SumAssured=100000;PropWOP=78543;PropTPD=100000;null;null;:POLICY 1 GENERAL#;null;null;:LIFE 1 POLICY 1 DECISION#;LIFE=Refer HIV RL1 URD;WOP=Refer CLC HIV RL1 URD;TPD=Refer CLC HIV OOD RL1 URD;Evi=HIV URD;null;:LIFE 1 GENERAL#;Abroad=False;IslandSequence=medical;SanQuoteKeyPerson=UnKnown;StandardExclusionsBackAndPsych=False;RateGroupSystem=0;Decision=S ;null;null;:LIFE 1 SUM ASSURED#;ExistingThisCompanyLife=0;ExistingThisCompanyLifeExp=0;ExistingOtherCompaniesLife=0;ExistingThisCompanyWOP=0;ExistingOtherCompaniesWOP=0;ExistingThisCompanyTPD=0;ExistingOtherCompaniesTPD=0;ExistingPensionTPD=0;ExistingThisCompanyCPI=0;ExistingOtherCompaniesCPI=0;ExistingThisCompanyFIP=0;ExistingThisCompanyPYI=0;ExistingThisCompanyDD=0;ExistingOtherCompaniesDD=0;ExistingThisCompanyACC=0;ExistingThisCompanyACCInjury=0;ExistingThisCompanyGIB=0;ExistingThisCompanyPHD=0;ExistingOtherCompaniesPHD=0;ExistingThisCompanyPHI=0;ExistingOtherCompaniesPHI=0;ExistingPensionPHI=0;ExistingThisCompanyTIP=0;ExistingOtherCompaniesTIP=0;ExistingThisCompanyBOP=0;ExistingOtherCompaniesBOP=0;KeymanExisting=UnKnown;TotalLifeSumAssuredPastTwoYears=278543;Decision=S ;null;null;:LIFE 1 BUILD#;Height=168;Weight=69;WeightChangeType=Unchanged;WeightChange=0;BodyMassIndex=24.447279;Decision=S ;null;null;:LIFE 1 ALCOHOL#;AlcoholUnitsPerWeek=1;AlcoholAdvice=False;Decision=S ;null;null;:LIFE 1 TOBACCO#;CigarettesPerDay=3;SmokedPastTwelveMonths=True;Decision=S ;null;null;:LIFE 1 HABIT COMBINATION#;Decision=S ;null;null;:LIFE 1 OCCUPATION 1#;Occupation=restourantbestuurder \\ eienaar;Concept=restauranthotelownersmanagers(noind);AdminWork=50;NonAdminManualLabour=0;NonAdminSupervision=0;NonAdminTravel=0;NonAdminWork=50;DisClass=3;AccClass=0;ClauseCodeOccupation=UnKnown;Decision=S ;null;null;:LIFE 1 OCCUPATION END#;OccupationHazardousPastFuture=False;Decision=S ;null;null;:LIFE 1 POLICY 1 OMS#;Decision=S ;null;null;:LIFE 1 POLICY 1 RATE GROUP#;RateGroupPolicy=1;null;RateGroup=1;RateGroupTop=1;RateGroupOccupationLIFE=0;Decision=S RL1;null;null;:LIFE 1 IMPAIRMENT 1#;Impairment=unrecognised;Concept=unrecognised;ProblemDate=30/06/1997;FullyRecovered=True;OnTreatment=False;Decision=R URD;null;null;:LIFE 1 IMPAIRMENT END#;AbsentFromWork=False;Decision=S ;null;null;:LIFE 1 OTHER MEDICAL#;OtherTreatment=False;PreviousMedicalEvidenceThisCompany=False;PreviouslyClaimed=False;SpecialInvestigation=False;Decision=S ;null;null;null;:LIFE 1 PREVIOUSLY DECLINED OR RATED#;PreviouslyDeclinedRated=False;ExistingMedicalClause=False;Decision=S ;null;null;:LIFE 1 AIDS QUESTIONNAIRE#;AidsQuestion=False;Decision=S ;null;null;:LIFE 1 POLICY 1 NON MEDICAL LIMITS#;Decision=S HIV;null;null;:LIFE 1 POLICY 1 FINANCIAL#;Decision=S ;null;null;:LIFE 2 POLICY 1 SPECIAL EVENTS#;LOAEntry=False;Decision=S ;null;null;null;";
        String str3 = ";null;:GLOBAL#;CompanyVersion=08.03.0053 (08/10/2007) 15:30;RunDate=21/11/2007;Environment=HOS;Language=English;Legacy=False;BlueSkyProduct=True;QuoteOnly=False;BrokerCode=00431150;null;null;:LIFE 4 OVERALL DECISION#;LIFE=Refer AQA COT FEF HIV HSP LOA MEB RL5;TPD=Refer AQA CLA COT HIV HSP LOA MEB OOD RL5;Evi=RU1;NMLEvidence=COT HIV MEB;null;:LIFE 1#;Name=BESTER MARTHINUS G.H.;Surname=BESTER;Forenames=MARTHINUS G.H.;Age=38;DateOfBirth=16/03/1970;AgeNextBirthday=38;Sex=Male;MaritalStatus=Single;Education=4-year Bachelor's Degree (university);IncomeLifeAssured=600000;IncomeSpouseGuardian=0;IncomeRateGroup=600000;IncomeFinancial=600000;UnderwritingType=UnKnown;null;null;:LIFE 1 POLICY 1 ALLOWABLE COVER#;AllowableLIFEAmount=3000000;AllowableDisabilityAmount=5000000;null;:LIFE 1 POLICY 1 EXCEEDED COVER#;ExceededLIFEAmount=0;ExceededDisabilityAmount=0;null;:POLICY 1#;PolicyName=0425092046;PolicyType=T02W;PolicyOwner=LIFE 1;SecondOwner=False;InsurableInterest=Own life;SecurityForLoan=False;FutureCoverOption=False;SourcePlanSmokerStatus=False;Premium=686.97;ProposedLife=Life 1;SumAssured=3750000;PropTPD=1875000;null;null;:POLICY 1 GENERAL#;null;null;:LIFE 1 POLICY 1 DECISION#;LIFE=Refer AQA COT FEF HIV HSP LOA MEB RL5;TPD=Refer AQA CLA COT HIV HSP LOA MEB OOD RL5;Evi=RU1;null;:LIFE 1 GENERAL#;Abroad=False;IslandSequence=medical;SanQuoteKeyPerson=UnKnown;StandardExclusionsBackAndPsych=False;RateGroupSystem=0;Decision=S ;null;null;:LIFE 1 SUM ASSURED#;ExistingThisCompanyLife=0;ExistingThisCompanyLifeExp=0;ExistingOtherCompaniesLife=6000000;ExistingThisCompanyWOP=0;ExistingOtherCompaniesWOP=0;ExistingThisCompanyTPD=0;ExistingOtherCompaniesTPD=3000000;ExistingPensionTPD=0;ExistingThisCompanyCPI=0;ExistingOtherCompaniesCPI=0;ExistingThisCompanyFIP=0;ExistingThisCompanyPYI=0;ExistingThisCompanyDD=0;ExistingOtherCompaniesDD=1000000;ExistingThisCompanyACC=0;ExistingThisCompanyACCInjury=0;ExistingThisCompanyGIB=0;ExistingThisCompanyPHD=0;ExistingOtherCompaniesPHD=0;ExistingThisCompanyPHI=0;ExistingOtherCompaniesPHI=0;ExistingPensionPHI=0;ExistingThisCompanyTIP=0;ExistingOtherCompaniesTIP=0;ExistingThisCompanyBOP=0;ExistingOtherCompaniesBOP=0;KeymanExisting=UnKnown;TotalLifeSumAssuredPastTwoYears=3750000;Decision=S ;null;null;:LIFE 1 BUILD#;Height=185;Weight=89;WeightChangeType=Unchanged;WeightChange=0;BodyMassIndex=26.004383;Decision=S ;null;null;:LIFE 1 ALCOHOL#;AlcoholUnitsPerWeek=0;AlcoholAdvice=False;Decision=S ;null;null;:LIFE 1 TOBACCO#;CigarettesPerDay=0;SmokedPastTwelveMonths=False;Decision=S ;null;null;:LIFE 1 HABIT COMBINATION#;Decision=S ;null;null;:LIFE 1 OCCUPATION 1#;Occupation=company director of corporate institution;Concept=administratorexecutive(noind);AdminWork=85;NonAdminManualLabour=0;NonAdminSupervision=0;NonAdminTravel=15;NonAdminWork=UnKnown;DisClass=1;AccClass=0;ClauseCodeOccupation=UnKnown;Decision=S ;null;null;:LIFE 1 OCCUPATION END#;OccupationHazardousPastFuture=False;Decision=S ;null;null;:LIFE 1 POLICY 1 OMS#;Decision=S ;null;null;:LIFE 1 POLICY 1 RATE GROUP#;RateGroupPolicy=5;null;RateGroup=5;RateGroupTop=5;RateGroupOccupationLIFE=0;Decision=S RL5;null;null;:LIFE 1 AVOCATION 1#;Avocation=diving (all forms);Concept=diving;ClauseCodeAvocation=31;AvoCode=D2;DivingQuestionnaire=1;DivingSportMethod=Snorkel and SCUBA diving;DivingOrganisation=None of the above;Decision=R HSP;null;Wording=Do you have a Diving Questionnaire?;Answer=1;QuestionID=DivingQuestionnaire;Wording=Which type of diving do you participate in?;Answer=Snorkel and SCUBA diving;QuestionID=DivingSportMethod;Wording=Under which training body did you train or which equivalent qualification do you hold?;Answer=None of the above;QuestionID=DivingOrganisation;null;:LIFE 1 IMPAIRMENT 1#;Impairment=asma;Concept=asthma;ProblemDate=30/06/1998;FullyRecovered=UnKnown;OnTreatment=True;Decision=R AQA;null;null;:LIFE 1 IMPAIRMENT END#;AbsentFromWork=False;Decision=S ;null;null;:LIFE 1 OTHER MEDICAL#;OtherTreatment=False;PreviousMedicalEvidenceThisCompany=False;PreviouslyClaimed=False;SpecialInvestigation=False;Decision=S ;null;null;null;:LIFE 1 PREVIOUSLY DECLINED OR RATED#;PreviouslyDeclinedRated=False;ExistingMedicalClause=False;Decision=S ;null;null;:LIFE 1 AIDS QUESTIONNAIRE#;AidsQuestion=False;Decision=S ;null;null;:LIFE 1 POLICY 1 NON MEDICAL LIMITS#;Decision=R COT HIV SMR;null;null;:LIFE 1 POLICY 1 FINANCIAL#;Decision=R FEF;null;null;:LIFE 1 POLICY 1 SPECIAL EVENTS#;LOAEntry=True;Decision=R LOA;null;null;null;";

        List<String> strings = new ArrayList<>();
        strings.add(str);
        strings.add(str2);
        strings.add(str3);

        // Create a Spark session on local cluster
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> rddList = sc.parallelize(strings);

        JavaPairRDD<String, String> javaPairRDD = rddList.mapToPair(getLifeAndPolicy());

        javaPairRDD.foreach(data -> System.out.println("Key ==> " + data._1() + " Value ==> " + data._2()));
    }


    private static PairFunction<String, String, String> getLifeAndPolicy() {
        return (String s) -> {

            StringBuilder cleanSentencesKey = new StringBuilder();
            StringBuilder cleanSentencesValue = new StringBuilder();

            String sentences = s
                    .replaceAll("null", "") // we don't need nulls
                    .replaceAll(";;", "") // we only need a single occurence not double
                    .replaceAll("(\\d+)\\:(\\d+)", "00"); // we do not need this for now

            // split on colon then retrieve a key value map from the split on the hash (#)
            // exclude whitespaces
            Map<String, String> map = Splitter.on(":")
                    .omitEmptyStrings()
                    .trimResults()
                    .withKeyValueSeparator("#")
                    .split(sentences);

            map.forEach((key,value) -> {
                cleanSentencesKey.append(key + " " + value);
//                cleanSentencesValue.append(value + " ");
            });

//            System.out.println(cleanSentencesKey.toString());
            return new Tuple2<>(cleanSentencesKey.toString(), "");
        };
    }
}
