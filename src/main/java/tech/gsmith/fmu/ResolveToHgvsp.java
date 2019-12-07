package tech.gsmith.fmu;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;

/**
 *
 * @author pyewacket
 */
public class ResolveToHgvsp {

    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    public static class VEP {
        public List<TC> transcript_consequences;
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TC {
        public String canonical;
        public String hgvsp;
    }

    public static ClientConfig cc;
    public static Client client;
    static {
        cc = new ClientConfig().connectorProvider(new ApacheConnectorProvider());        
        cc.property(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.BUFFERED);
        client = ClientBuilder.newClient(cc);
    }
    
    public static String getHgvsp(String hgvsc) {
        List<VEP> veps = client
            .target(String.format("https://grch37.rest.ensembl.org/vep/human/hgvs/%s?canonical=1&hgvs=1&content-type=application/json", hgvsc))
            .request(MediaType.APPLICATION_JSON)
            .get(new GenericType<List<VEP>>(){});
        for(VEP vep : veps) {
            for(TC tc : vep.transcript_consequences) {
                if("1".equals(tc.canonical)) {
                    if(tc.hgvsp != null && tc.hgvsp.matches("^.*:p.*$")) {
                        return hgvsc.replaceAll(":.*$", "") + ":" + tc.hgvsp.replaceAll("^.*:", "");
                    }
                    else {
                        return hgvsc;
                    }
                }
            }
        }
        return null;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

        Properties priv = new Properties();
        try(InputStream inputStream = MergeWithCp.class.getClassLoader().getResourceAsStream("private.properties")) {
            priv.load(inputStream);
        }
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection conn = DriverManager.getConnection(priv.getProperty("connCoPath.url"));
        
        CaseAttributesFinder caf = new CaseAttributesFinder(conn);

        CSVParser csvParser = CSVParser.parse(new File("/home/pyewacket/in.tsv"), Charset.defaultCharset(), CSVFormat.TDF.withFirstRecordAsHeader());

        System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
            "SOURCE"
           ,"ORDER_ID"
           ,"ACC_NO"
           ,"LAST_NAME_FROM_COPATH"
           ,"EMPI_FROM_COPATH"
           ,"MRN_FROM_COPATH"
           ,"PANEL"
           ,"SIGNOUT_DATE"
           ,"PRIMARY_TUMOR_TYPE"
           ,"COUNT_VARIANT_GENES"
           ,"KRAS"
           ,"TP53"
           ,"STK11"
           ,"FULL_ALTERATION_LIST"
        ));
        for(CSVRecord csvRecord : csvParser) {
            String[] hgvscs = csvRecord.get("FULL_ALTERATION_LIST").split(", ");
            List<String> hgvsps = new ArrayList<>();
            if("pre-GenomOncology".equals(csvRecord.get("SOURCE"))) {
                for(String hgvsc : hgvscs) {
                    hgvsps.add(getHgvsp(hgvsc));
                }
            }
            else {
                for(String hgvsc : hgvscs) {
                    hgvsps.add(hgvsc);
                }
            }
            CaseAttributes ca = null;
            if(!csvRecord.get("SOURCE").contains("[non-EHC case?]")) {
                ca = caf.getByAccessionNumber(csvRecord.get("ACC_NO"));
            }
            System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
                csvRecord.get("SOURCE")
               ,csvRecord.get("ORDER_ID")
               ,csvRecord.get("ACC_NO")
               ,(ca != null ? ca.lastName : "?")
               ,(ca != null ? ca.empi : "?")
               ,(ca != null ? ca.mrn : "?")
               ,csvRecord.get("PANEL")
               ,csvRecord.get("SIGNOUT_DATE")
               ,csvRecord.get("PRIMARY_TUMOR_TYPE")
               ,csvRecord.get("COUNT_VARIANT_GENES")
               ,csvRecord.get("KRAS")
               ,csvRecord.get("TP53")
               ,csvRecord.get("STK11")
               ,String.join(", ", hgvsps)
            ));
        }
        
    }
    
}
