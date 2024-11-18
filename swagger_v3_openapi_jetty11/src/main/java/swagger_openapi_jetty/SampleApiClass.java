package swagger_openapi_jetty;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@OpenAPIDefinition(
        info = @Info(
                title = "MyTitle",
                version = "1.0.0",
                description = "API Documentation",
                license = @License(name = "Test License", url = "https://example.com"),
                contact = @Contact(url = "https://example.com/contact")
        )
)
@Path("/sampleapi/")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class SampleApiClass extends AbstractHandler {

    static final ObjectMapper MAPPER = new ObjectMapper();

    final String GET_REQUEST = "GET";
    final String POST_REQUEST = "POST";
    final String RESPONSE_CONTENT_TYPE_JSON = "application/json;charset=UTF-8";
    final String RESPONSE_CONTENT_TYPE_TEXT = "text/html;charset=UTF-8";

    @GET
    @Path("ping")
    @Produces(MediaType.TEXT_PLAIN)
    public String ping() {
        return "Ping response";
    }

    @POST
    @Path("suggestions")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    /*@RequestBody(
            description = "RequestBody List<String>",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = JsonArray.class)))*/
    public List<String> suggestions(List<String> request) throws Exception {
        List<String> r = new ArrayList<>();
        r.add("response");
        r.addAll(request);
        return r;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "SampleApi",
            description = "returns a list of clients"
    )
    @ApiResponse(content = @Content(mediaType = "application/json"))
    @ApiResponse(responseCode = "200", description = "Ok")
    @ApiResponse(responseCode = "400", description = "Bad Request")
    @ApiResponse(responseCode = "404", description = "Error")
    @ApiResponse(responseCode = "500", description = "Internal Server Error")
    @ApiResponse(responseCode = "503", description = "Service Unavailable")
    @Tag(name = "MyApi")
    public void sampleApi(@Context Request jettyRequest, @Context HttpServletRequest request,
                          @Context HttpServletResponse response) throws IOException {

    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse response) throws IOException, ServletException {
        if (request.getMethod().equals(GET_REQUEST)) {
            if (s.equals("/ping")) {
                String s0 = ping();

                response.setContentType(RESPONSE_CONTENT_TYPE_TEXT);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().println(s0);
            }
        } else if (request.getMethod().equals(POST_REQUEST)) {
            try {
                if (s.equals("/suggestions")) {
                    String requestBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
                    String[] ss = MAPPER.readValue(requestBody, String[].class);

                    List<String> s0 = suggestions(Arrays.asList(ss));

                    response.setContentType(RESPONSE_CONTENT_TYPE_TEXT);
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.getWriter().println(s0);
                }
            } catch (Exception e) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType(RESPONSE_CONTENT_TYPE_TEXT);
                response.getWriter().println(e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
        request.setHandled(true);
    }

}