package tech.gsmith.fmu;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;

public class Download {

    public static class Cred {
        public String email;
        public String password;
        public Cred(String email, String password) {
            this.email = email;
            this.password = password;
        }
    }
    
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
        public List<Gene> genes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Gene {
        public String name;
        public List<String> alterations;
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {

        PrintWriter table1 = new PrintWriter(new File("table1.tsv"));
        PrintWriter table2 = new PrintWriter(new File("table2.tsv"));
        
        ClientConfig cc = new ClientConfig().connectorProvider(new ApacheConnectorProvider());        
        cc.property(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.BUFFERED);
        Client client = ClientBuilder.newClient(cc);
        
        {
            Thread.sleep(1000);
            Response response = client
                .target("https://www.***/api/v1/login")
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(new Cred("***", "***"), MediaType.APPLICATION_JSON));
            System.err.println(response);
        }
        
        ObjectMapper mapper = new ObjectMapper();
        F f = mapper.readValue(new File(args[0]), F.class);
        F f_exclude = mapper.readValue(new File(args[1]), F.class);
        
        table1.println(String.format(
            "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
            "accNo",
            "trf",
            "dateAndTime",
            "d",
            "tt",
            "countCaseGenes",
            "countCaseAlterations",
            "genesAndAlterations"
        ));
        
        table2.println(String.format(
            "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
            "accNo",
            "trf",
            "dateAndTime",
            "d",
            "tt",
            "countCaseGenes",
            "countCaseAlterations",
            "gene",
            "alteration"
        ));

        int count = 0;
        
        for(Report report : f.reports) {

            if(!report.tt.startsWith("***")) {
                continue;
            }

            if(f_exclude.reports.stream().filter(r -> (r.trf.equals(report.trf))).findAny().isPresent()) {
                System.err.println(String.format("skipping %s", report.trf));
                continue;
            }
            
            String accNo;
            
            {
                Thread.sleep(1000);
                Response response = client
                    .target("https://reporting.***/api/v2/report/download/" + report.trf)
                    .request(MediaType.APPLICATION_OCTET_STREAM)
                    .get();
                System.err.println(response);
                InputStream inputStream = response.readEntity(InputStream.class);
                File file = new File(report.trf + ".pdf");
                java.nio.file.Files.copy(inputStream, file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                response.close();
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
                file.renameTo(new File(String.format("%s_%s", accNo, file.getName())));
            }

            System.err.println(String.format("[%05d] %s %s", ++count, report.trf, accNo));

            table1.print(String.format(
                "%s\t%s\t%s\t%s\t%s\t%d\t%d",
                accNo,
                report.trf,
                report.dateAndTime,
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
                            "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s",
                            accNo,
                            report.trf,
                            report.dateAndTime,
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

        {
            Thread.sleep(1000);
            Response response = client
                .target("https://home.***/api/v1/session/logout")
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity("", MediaType.APPLICATION_JSON));
            System.err.println(response);
        }
        
        table1.close();
        table2.close();
        
    }
    
}
