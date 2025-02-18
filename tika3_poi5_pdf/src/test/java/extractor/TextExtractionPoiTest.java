package extractor;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TextExtractionPoiTest {
    private static final Log log = LogFactory.getLog(TextExtractionTikaTest.class);

    @Test
    public void test1() throws Exception {
        List<String> l = new ArrayList<>();
        l.add("2020-2021_q4_financial_presentation.pptx");
        l.add("1001160610.pdf");
        l.add("B049-19AttachC.docx");
        l.add("CMBARR_Civilian of the Quarter Instructions.doc");
        l.add("County Safety Committee 3rd Quarter 2019.pptx");
        l.add("covestro-q1-21-share-at-a-glance.xlsx");
        l.add("naics-based-quarterly-layout-xlsx.xlsx");
        l.add("qupd_fig1.xls");
        l.add("SABI_TEMPLATE - Case of the Quarter Submission.ppt");
        l.add("ssm.supervisorybankingstatistics_first_quarter_2019_201907_annex~0756086880.en.xlsx");
        l.add("TestVisioWithCodepage.vsd");
        l.add("ViewPressRelease.pdf");
        l.add("云存储完整性校验.vsdx");

        for (String s : l) {
            try (InputStream is = this.getClass().getResourceAsStream(s)) {
                String result = new TextExtractionPoi().parse(s, is);

                String expected = getResourceAsString(this.getClass(), s + ".ResultPoi.txt", StandardCharsets.UTF_8.name());
                // FileUtils.write(new File(s + ".ResultPoi.txt"), result, StandardCharsets.UTF_8.name());
                Assertions.assertEquals(expected, result);
            }
        }
    }

    public static String getResourceAsString(Class<?> clazz, String fileName, String encoding) {
        try (InputStream is = clazz.getResourceAsStream(fileName);) {
            return IOUtils.toString(is, encoding);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

}
