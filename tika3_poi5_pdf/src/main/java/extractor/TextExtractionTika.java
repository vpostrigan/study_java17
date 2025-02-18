package extractor;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.xml.sax.ContentHandler;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;

public class TextExtractionTika {

    private final String text;
    private final String xml;

    public TextExtractionTika(InputStream input) throws Exception {
        input = TikaInputStream.get(input);

        StringWriter textBuffer = new StringWriter();
        StringWriter xmlBuffer = new StringWriter();

        ContentHandler handler = new TeeContentHandler(
                getTextContentHandler(textBuffer),
                getXmlContentHandler(xmlBuffer));

        Metadata md = new Metadata();

        ParseContext context = new ParseContext();

        TikaConfig config = TikaConfig.getDefaultConfig();
        Parser parser = new AutoDetectParser(config);

        parser.parse(input, handler, md, context);

        text = textBuffer.toString();
        xml = xmlBuffer.toString();
    }

    private ContentHandler getTextContentHandler(Writer writer) {
        return new BodyContentHandler(writer);
    }

    private ContentHandler getXmlContentHandler(Writer writer) throws TransformerConfigurationException {
        SAXTransformerFactory factory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();

        TransformerHandler handler = factory.newTransformerHandler();
        handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "xml");
        handler.setResult(new StreamResult(writer));

        return handler;
    }

    public String getText() {
        return text;
    }

    public String getXml() {
        return xml;
    }

}
