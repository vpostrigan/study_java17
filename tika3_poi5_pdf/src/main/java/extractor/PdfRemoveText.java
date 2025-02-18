package extractor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfCopy;
import com.itextpdf.text.pdf.PdfImportedPage;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.pdfcleanup.PdfCleanUpLocation;
import com.itextpdf.text.pdf.pdfcleanup.PdfCleanUpProcessor;

public class PdfRemoveText {

    public static void main(String[] args) throws IOException, DocumentException {
        // manipulatePdf("d://clean_arch(1)_4.pdf", "d://clean_arch(1)_2.pdf");
        manipulatePdf("d://Effective.Java.3rd.Edition.2018.1.pdf", "d://Effective.Java.3rd.Edition.2018.1_2.pdf");
    }

    static void manipulatePdf(String src, String dest) throws IOException, DocumentException {
        PdfReader reader = new PdfReader(src);
        PdfStamper stamper = new PdfStamper(reader, new FileOutputStream(dest));

        int pagesCount = reader.getNumberOfPages();

        // 6 inch x 72 points = 432 points (the width)
        // 3.5 inch x 252 points = 252 points (the height)
        // Rectangle envelope = new Rectangle(432, 252);

        List<PdfCleanUpLocation> cleanUpLocations = new ArrayList<PdfCleanUpLocation>();
        for (int page = 1; page <= pagesCount; page++) {
            cleanUpLocations.add(
                    new PdfCleanUpLocation(page, new Rectangle(175f, 5f, 375f, 40f), BaseColor.WHITE/* BaseColor.GRAY */));
        }
        PdfCleanUpProcessor cleaner = new PdfCleanUpProcessor(cleanUpLocations, stamper);
        cleaner.cleanUp();
        stamper.close();
        reader.close();
    }

    static class MyReader extends PdfReader {
        public MyReader(String filename) throws IOException {
            super(filename);
        }

        public void decryptOnPurpose() {
            encrypted = false;
        }
    }

    static void removePages(String src, String dest) throws IOException, DocumentException {
        MyReader.unethicalreading = true;
        MyReader reader = new MyReader(src);
        reader.decryptOnPurpose();

        OutputStream outputStream = new FileOutputStream(dest);
        Document document = new Document();
        PdfCopy copy = new PdfCopy(document, outputStream);
        document.open();
        PdfStamper stamper = new PdfStamper(reader, outputStream);
        stamper.setEncryption(null, "".getBytes(), PdfWriter.ALLOW_PRINTING,
                PdfWriter.ENCRYPTION_AES_128 | PdfWriter.DO_NOT_ENCRYPT_METADATA);

        for (int i = 1; i < reader.getNumberOfPages(); i++) {
            if (i == 1 || i == 2 || i == 3 || i == 5 || i == 6 || i == 7) {
                continue;
            }
            // Select what pages you need here
            PdfImportedPage importedPage = stamper.getImportedPage(reader, i);
            copy.addPage(importedPage);
        }
        copy.freeReader(reader);
        outputStream.flush();
        document.close();
    }

}
