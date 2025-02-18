package extractor;

import org.apache.pdfbox.io.RandomAccessReadBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.extractor.POITextExtractor;
import org.apache.poi.hdgf.extractor.VisioTextExtractor;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextParagraph;
import org.apache.poi.hssf.extractor.ExcelExtractor;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.ooxml.POIXMLDocumentPart;
import org.apache.poi.ooxml.extractor.POIXMLTextExtractor;
import org.apache.poi.sl.extractor.SlideShowExtractor;
import org.apache.poi.xdgf.extractor.XDGFVisioExtractor;
import org.apache.poi.xdgf.usermodel.XmlVisioDocument;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xssf.extractor.XSSFExcelExtractor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.xmlbeans.XmlCursor;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;

import javax.swing.text.DefaultStyledDocument;
import javax.swing.text.rtf.RTFEditorKit;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * https://github.com/mariosotil/text-extractor/blob/master/src/main/java/org/riverframework/utils/TextExtractor.java
 */
public class TextExtractionPoi {
    private static final Log log = LogFactory.getLog(TextExtractionPoi.class);

    public enum Type {PDF, PPT, PPTX, VSD, VSDX, XLSX, XLS, DOC, DOCX, RTF}

    public String parse(String reference, InputStream is) {
        return parse(referenceToType(reference), is);
    }

    private Type referenceToType(String file0) {
        Type type = null;
        String file = file0.toLowerCase();

        if (file.endsWith(".pdf") || file.equals("application/pdf")) {
            type = Type.PDF;
        } else if (file.endsWith(".doc") || file.equals("application/msword")) {
            type = Type.DOC;
        } else if (file.endsWith(".docx") || file.equals("application/vnd.openxmlformats-officedocument.wordprocessingml.document")) {
            type = Type.DOCX;
        } else if (file.endsWith(".rtf") || file.equals("application/rtf")) {
            type = Type.RTF;
        } else if (file.endsWith(".xls") || file.equals("application/vnd.ms-excel")) {
            type = Type.XLS;
        } else if (file.endsWith(".xlsx") || file.equals("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) {
            type = Type.XLSX;
        } else if (file.endsWith(".ppt") || file.equals("application/vnd.ms-powerpoint")) {
            type = Type.PPT;
        } else if (file.endsWith(".pptx") || file.equals("application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
            type = Type.PPTX;
        } else if (file.endsWith(".vsd") || file.equals("application/vnd.visio")) {
            type = Type.VSD;
        } else if (file.endsWith(".vsdx") || file.equals("application/vnd.ms-visio.drawing.main+xml")) {
            type = Type.VSDX;
        }

        if (type == null)
            throw new RuntimeException("There's no a valid reference to detect the type of the input stream.");

        return type;
    }

    public String parse(Type type, InputStream is) {
        String result = "";

        try {
            if (type.equals(Type.PDF)) {
                result = pdf2text(is).stream().collect(Collectors.joining("\n"));
            } else if (type.equals(Type.DOC)) {
                result = doc2text(is);
            } else if (type.equals(Type.DOCX)) {
                result = docx2text(is);
            } else if (type.equals(Type.RTF)) {
                result = rtf2text(is);
            } else if (type.equals(Type.XLS)) {
                result = xls2text(is);
            } else if (type.equals(Type.XLSX)) {
                result = xlsx2text(is);
            } else if (type.equals(Type.PPT)) {
                result = ppt2text(is);
            } else if (type.equals(Type.PPTX)) {
                result = pptx2text(is);
            } else if (type.equals(Type.VSD)) {
                result = vsd2text(is);
            } else if (type.equals(Type.VSDX)) {
                result = vsdx2text(is);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public List<String> pdf2text(InputStream is) {
        List<String> pages = new ArrayList<>();

        PDDocument pdDocument = null;
        try {
            String pass = "";
            PDFParser parser = new PDFParser(new RandomAccessReadBuffer(is), pass);
            pdDocument = parser.parse();

            int numberOfPages = pdDocument.getNumberOfPages();

            // //

            for (int i = 0; i < numberOfPages; i++) {
                PDFTextStripper stripper = new PDFTextStripper();
                stripper.setStartPage(i + 1);
                stripper.setEndPage(i + 1);

                pages.add(stripper.getText(pdDocument));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (pdDocument != null) {
                try {
                    pdDocument.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        return pages;
    }

    public String xls2text(InputStream is) throws IOException {
        ExcelExtractor wd = new ExcelExtractor(new HSSFWorkbook(is));
        String text = wd.getText();
        wd.close();
        return text;
    }

    public String xlsx2text(InputStream is) throws Exception {
        XSSFWorkbook wb = new XSSFWorkbook(is);
        XSSFExcelExtractor we = new XSSFExcelExtractor(wb);
        // we.setFormulasNotResults(true);
        // we.setIncludeSheetNames(false);
        String text = we.getText();
        we.close();
        return text;
    }

    public String doc2text(InputStream is) throws IOException {
        WordExtractor wd = new WordExtractor(is);
        String text = wd.getText();
        wd.close();
        return text;
    }

    public String docx2text(InputStream is) throws IOException {
        XWPFDocument doc = new XWPFDocument(is);
        XWPFWordExtractor we = new XWPFWordExtractor(doc);
        String text = we.getText();
        we.close();
        return text;
    }


    public String rtf2text(InputStream is) throws Exception {
        DefaultStyledDocument styledDoc = new DefaultStyledDocument();
        new RTFEditorKit().read(is, styledDoc, 0);
        return styledDoc.getText(0, styledDoc.getLength());
    }

    /**
     * https://github.com/ComradDart/Handler/blob/master/src/main/java/dart/handler/handlers/PowerPointHandler.java
     */
    public String ppt2text(InputStream is) throws IOException, XmlException {
        HSLFSlideShow ppt = new HSLFSlideShow(is);
        SlideShowExtractor<HSLFShape, HSLFTextParagraph> slideShowExtractor = new SlideShowExtractor<>(ppt);
        slideShowExtractor.setCommentsByDefault(true);
        slideShowExtractor.setMasterByDefault(true);
        slideShowExtractor.setNotesByDefault(true);

        return slideShowExtractor.getText();
    }

    /**
     * https://github.com/Assleep/SpS/blob/master/src/Parser.java
     */
    public String pptx2text(InputStream is) throws IOException, XmlException {
        XMLSlideShow ppt = new XMLSlideShow(is);
        SlideShowExtractor<XSLFShape, XSLFTextParagraph> slideShowExtractor = new SlideShowExtractor<XSLFShape, XSLFTextParagraph>(ppt);
        StringBuilder allTextInSlide = new StringBuilder("");

        slideShowExtractor.setCommentsByDefault(true);
        slideShowExtractor.setMasterByDefault(true);
        slideShowExtractor.setNotesByDefault(true);
        slideShowExtractor.setSlidesByDefault(true);

        StringBuilder sb = new StringBuilder();
        for (XSLFSlide slide : ppt.getSlides()) {
            for (POIXMLDocumentPart part : slide.getRelations()) {
                if (part.getPackagePart().getPartName().getName().startsWith("/ppt/diagrams/data")) {
                    XmlCursor cursor = XmlObject.Factory.parse(part.getPackagePart().getInputStream()).newCursor();
                    while (cursor.hasNextToken()) {
                        if (cursor.isText()) {
                            sb.append(cursor.getTextValue() + "\r\n");
                        }
                        cursor.toNextToken();
                    }
                }
            }
        }

        String allTextContentInSlideShow = slideShowExtractor.getText();
        POITextExtractor textExtractor = slideShowExtractor.getMetadataTextExtractor();
        String metaData = textExtractor.getText();

        String allTextContentInDiagrams = sb.toString();
        allTextInSlide.append(allTextContentInDiagrams).append("\n");
        //allTextInSlide.append(metaData).append("\n");
        allTextInSlide.append(allTextContentInSlideShow);
        return allTextInSlide.toString();
    }

    public String vsd2text(InputStream is) throws IOException, XmlException {
        VisioTextExtractor wd = new VisioTextExtractor(is);
        String text = wd.getText();
        wd.close();
        return text;
    }

    public String vsdx2text(InputStream is) throws IOException, XmlException {
        XmlVisioDocument doc = new XmlVisioDocument(is);
        POIXMLTextExtractor extractor = new XDGFVisioExtractor(doc);
        String text = extractor.getText();
        extractor.close();
        return text;
    }

}
