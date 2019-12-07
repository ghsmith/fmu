package tech.gsmith.fmu;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

public class MergeWithCp {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class F {
        public Map<String, String> physicians;
        public Map<String, String> facilities;
        public List<Report> reports;
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Report {
        public String trf;
        public String dateAndTime;
        public String d;
        public String tt;
        public String pL;
        public List<Gene> genes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Gene {
        public String name;
        public List<String> alterations;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, SQLException {

        Properties priv = new Properties();
        try(InputStream inputStream = MergeWithCp.class.getClassLoader().getResourceAsStream("private.properties")) {
            priv.load(inputStream);
        }
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection conn = DriverManager.getConnection(priv.getProperty("connCoPath.url"));
        
        CaseAttributesFinder caf = new CaseAttributesFinder(conn);
        
        PrintWriter table1 = new PrintWriter(new File("table1.tsv"));
        PrintWriter table2 = new PrintWriter(new File("table2.tsv"));
        
        ObjectMapper mapper = new ObjectMapper();
        F f = mapper.readValue(new File(args[0]), F.class);

        table1.println(String.format(
            "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
            "ehcCase",
            "mrn",
            "empi",
            "accNo",
            "cmp26",
            "mmp75",
            "trf",
            "date",
            "d",
            "tt",
            "countCaseGenes",
            "countCaseAlterations",
            "genesAndAlterations"
        ));
        
        table2.println(String.format(
            "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
            "ehcCase",
            "mrn",
            "accNo",
            "trf",
            "date",
            "d",
            "tt",
            "countCaseGenes",
            "countCaseAlterations",
            "gene",
            "alteration"
        ));

        int count = 0;
        
        for(Report report : f.reports) {

            if(!report.tt.startsWith("FoundationOne")) {
                continue;
            }

            String accNo;
            File file;

            {
                File[] files = new File(".").listFiles((File pathname) -> (pathname.getName().endsWith(String.format("%s.pdf", report.trf))));
                if(files.length != 1) {
                    System.err.println(String.format("*** missing file: %s ***", report.trf));
                    continue;
                }
                file = files[0];
                PDDocument document = PDDocument.load(file);
                PDFTextStripper stripper = new PDFTextStripper();
                stripper.setSortByPosition(true);
                stripper.setStartPage(1);
                stripper.setEndPage(1);
                String text = stripper.getText(document);
                Pattern pat = Pattern.compile("(SPECIMEN ID|Specimen ID|Block ID) *([A-Z][A-Z]?[A-Z]?[0-9][0-9]-[0-9]*)");
                Matcher mat = pat.matcher(text);
                if(mat.find()) {
                    accNo = mat.group(2);
                }
                else {
                    accNo = "UNKNOWN";
                }
                document.close();
            }

            CaseAttributes ca = caf.getByAccessionNumber(accNo);
            Boolean ehcCase = false;
            if(ca != null && ca.lastName != null && report.pL != null) {
                ehcCase = ca.lastName.toUpperCase().trim().replaceAll(" ", "").equals(report.pL.toUpperCase().trim().replaceAll(" ", ""));
            }
            
            System.err.println(String.format("[%05d] %s %s '%s'=='%s' --> %s", ++count, report.trf, accNo, report.pL, ca != null ? ca.lastName : "case-not-found", ehcCase));

            {
                PDDocument document = PDDocument.load(file);
                PDFTextStripper stripper = new PDFTextStripper();
                for(int pageNo = 1; pageNo <= document.getNumberOfPages(); pageNo++) {
                    stripper.setSortByPosition(false);
                    stripper.setAddMoreFormatting(true);
                    stripper.setStartPage(pageNo);
                    stripper.setEndPage(pageNo);
                    String text = stripper.getText(document);
                    if(Pattern.compile("ONE OR MORE VARIANTS OF UNKNOWN SIGNIFICANCE", Pattern.CASE_INSENSITIVE).matcher(text).find()) {
                        BufferedReader lines = new BufferedReader(new StringReader(text));
                        String line;
                        while((line = lines.readLine()) != null) {
                            if(report.genes == null) {
                                report.genes = new ArrayList<>();
                                System.err.println("Warning! Gene list not natively instantiated.");
                            }
                            if(genes.contains(line)) {
                                Gene gene;
                                if(report.genes.indexOf(line) == -1) {
                                    gene = new Gene();
                                    gene.name = line;
                                    gene.alterations = new ArrayList<>();
                                    report.genes.add(gene);
                                }
                                else {
                                    gene = report.genes.get(report.genes.indexOf(line));
                                }
                                while((line = lines.readLine()) != null) {
                                    if(line.equals("")) {
                                        break;
                                    }
                                    for(String alteration : line.split("(,|and)")) {
                                        gene.alterations.add(String.format("(VUS) %s", alteration.trim()));
                                    }
                                }
                            }
                        }
                    }
                }
                document.close();
            }

            table1.print(String.format(
                "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d",
                ehcCase,
                ehcCase ? ca.mrn : "",
                ehcCase ? ca.empi : "",
                accNo,
                ehcCase ? ca.cmp26 : "",
                ehcCase ? ca.mmp75 : "",
                report.trf,
                report.dateAndTime.substring(0, 10),
                report.d,
                report.tt,
                report.genes != null ? report.genes.size() : 0,
                report.genes != null
                    ? new Object() {
                        int doIt() {
                            int countAlterations = 0;
                            for(Gene gene : report.genes) {
                                countAlterations += gene.alterations.size();
                            }
                            return countAlterations;
                        }
                    }.doIt()
                    : 0    
            ));
            if(report.genes != null) {
                for(Gene gene : report.genes) {
                    table1.print(String.format("\t%s\t%s", gene.name, gene.alterations));
                }
            }
            table1.println();
            table1.flush();
            
            if(report.genes != null) {
                for(Gene gene : report.genes) {
                    for(String alteration : gene.alterations) {
                        table2.print(String.format(
                            "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s",
                            ehcCase,
                            ehcCase ? ca.mrn : "",
                            ehcCase ? ca.empi : "",
                            accNo,
                            ehcCase ? ca.cmp26 : "",
                            ehcCase ? ca.mmp75 : "",
                            report.trf,
                            report.dateAndTime.substring(0, 10),
                            report.d,
                            report.tt,
                            report.genes.size(),
                            new Object() {
                                int doIt() {
                                    int countAlterations = 0;
                                    for(Gene gene : report.genes) {
                                        countAlterations += gene.alterations.size();
                                    }
                                    return countAlterations;
                                }
                            }.doIt(),
                            gene.name,
                            alteration
                        ));
                        table2.println();
                        table2.flush();
                    }
                    
                }
                
            }

        }

        table1.close();
        table2.close();
        
    }

static List<String> genes = Arrays.asList(new String[] {
 "ABL1"
,"ABL2"
,"ACVR1B"
,"AKT1"
,"AKT2"
,"AKT3"
,"ALK"
,"AMER1"
,"APC"
,"AR"
,"ARAF"
,"ARFRP1"
,"ARID1A"
,"ARID1B"
,"ARID2"
,"ASXL1"
,"ATM"
,"ATR"
,"ATRX"
,"AURKA"
,"AURKB"
,"AXIN1"
,"AXL"
,"BAP1"
,"BARD1"
,"BCL2"
,"BCL2L1"
,"BCL2L2"
,"BCL6"
,"BCOR"
,"BCORL1"
,"BLM"
,"BRAF"
,"BRCA1"
,"BRCA2"
,"BRD4"
,"BRIP1"
,"BTG1"
,"BTK"
,"C11orf30"
,"C17orf39"
,"CARD11"
,"CBFB"
,"CBL"
,"CCND1"
,"CCND2"
,"CCND3"
,"CCNE1"
,"CD274"
,"CD79A"
,"CD79B"
,"CDC73"
,"CDH1"
,"CDK12"
,"CDK4"
,"CDK6"
,"CDK8"
,"CDKN1A"
,"CDKN1B"
,"CDKN2A"
,"CDKN2B"
,"CDKN2C"
,"CEBPA"
,"CHD2"
,"CHD4"
,"CHEK1"
,"CHEK2"
,"CIC"
,"CREBBP"
,"CRKL"
,"CRLF2"
,"CSF1R"
,"CTCF"
,"CTNNA1"
,"CTNNB1"
,"CUL3"
,"CYLD"
,"DAXX"
,"DDR2"
,"DICER1"
,"DNMT3A"
,"DOT1L"
,"EGFR"
,"EMSY"
,"EP300"
,"EPHA3"
,"EPHA5"
,"EPHA7"
,"EPHB1"
,"ERBB2"
,"ERBB3"
,"ERBB4"
,"ERG"
,"ERRFI1"
,"ESR1"
,"EZH2"
,"FAM123B"
,"FAM46C"
,"FANCA"
,"FANCC"
,"FANCD2"
,"FANCE"
,"FANCF"
,"FANCG"
,"FANCL"
,"FAS"
,"FAT1"
,"FBXW7"
,"FGF10"
,"FGF14"
,"FGF19"
,"FGF23"
,"FGF3"
,"FGF4"
,"FGF6"
,"FGFR1"
,"FGFR2"
,"FGFR3"
,"FGFR4"
,"FH"
,"FLCN"
,"FLT1"
,"FLT3"
,"FLT4"
,"FOXL2"
,"FOXP1"
,"FRS2"
,"FUBP1"
,"GABRA6"
,"GATA1"
,"GATA2"
,"GATA3"
,"GATA4"
,"GATA6"
,"GID4"
,"GLI1"
,"GNA11"
,"GNA13"
,"GNAQ"
,"GNAS"
,"GPR124"
,"GRIN2A"
,"GRM3"
,"GSK3B"
,"H3F3A"
,"HGF"
,"HNF1A"
,"HRAS"
,"HSD3B1"
,"HSP90AA1"
,"IDH1"
,"IDH2"
,"IGF1R"
,"IGF2"
,"IKBKE"
,"IKZF1"
,"IL7R"
,"INHBA"
,"INPP4B"
,"IRF2"
,"IRF4"
,"IRS2"
,"JAK1"
,"JAK2"
,"JAK3"
,"JUN"
,"KAT6A"
,"KDM5A"
,"KDM5C"
,"KDM6A"
,"KDR"
,"KEAP1"
,"KEL"
,"KIT"
,"KLHL6"
,"KMT2A"
,"KMT2C"
,"KMT2D"
,"KRAS"
,"LMO1"
,"LRP1B"
,"LYN"
,"LZTR1"
,"MAGI2"
,"MAP2K1"
,"MAP2K2"
,"MAP2K4"
,"MAP3K1"
,"MCL1"
,"MDM2"
,"MDM4"
,"MED12"
,"MEF2B"
,"MEN1"
,"MET"
,"MITF"
,"MLH1"
,"MLL"
,"MLL2"
,"MLL3"
,"MPL"
,"MRE11A"
,"MSH2"
,"MSH6"
,"MTOR"
,"MUTYH"
,"MYC"
,"MYCL"
,"MYCL1"
,"MYCN"
,"MYD88"
,"MYST3"
,"NF1"
,"NF2"
,"NFE2L2"
,"NFKBIA"
,"NKX2-1"
,"NOTCH1"
,"NOTCH2"
,"NOTCH3"
,"NPM1"
,"NRAS"
,"NSD1"
,"NTRK1"
,"NTRK2"
,"NTRK3"
,"NUP93"
,"only"
,"PAK3"
,"PALB2"
,"PARK2"
,"PAX5"
,"PBRM1"
,"PDCD1LG2"
,"PDGFRA"
,"PDGFRB"
,"PDK1"
,"PIK3C2B"
,"PIK3CA"
,"PIK3CB"
,"PIK3CG"
,"PIK3R1"
,"PIK3R2"
,"PLCG2"
,"PMS2"
,"POLD1"
,"POLE"
,"PPP2R1A"
,"PRDM1"
,"PREX2"
,"PRKAR1A"
,"PRKCI"
,"PRKDC"
,"promoter"
,"PRSS8"
,"PTCH1"
,"PTEN"
,"PTPN11"
,"QKI"
,"RAC1"
,"RAD50"
,"RAD51"
,"RAF1"
,"RANBP2"
,"RARA"
,"RB1"
,"RBM10"
,"RET"
,"RICTOR"
,"RNF43"
,"ROS1"
,"RPTOR"
,"RUNX1"
,"RUNX1T1"
,"SDHA"
,"SDHB"
,"SDHC"
,"SDHD"
,"SETD2"
,"SF3B1"
,"SLIT2"
,"SMAD2"
,"SMAD3"
,"SMAD4"
,"SMARCA4"
,"SMARCB1"
,"SMO"
,"SNCAIP"
,"SOCS1"
,"SOX10"
,"SOX2"
,"SOX9"
,"SPEN"
,"SPOP"
,"SPTA1"
,"SRC"
,"STAG2"
,"STAT3"
,"STAT4"
,"STK11"
,"SUFU"
,"SYK"
,"TAF1"
,"TBX3"
,"TERC"
,"TERT"
,"TET2"
,"TGFBR2"
,"TNFAIP3"
,"TNFRSF14"
,"TOP1"
,"TOP2A"
,"TP53"
,"TSC1"
,"TSC2"
,"TSHR"
,"U2AF1"
,"VEGFA"
,"VHL"
,"WISP3"
,"WT1"
,"XPO1"
,"ZBTB2"
,"ZNF217"
,"ZNF703"
});
    
}
