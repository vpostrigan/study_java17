package swagger_openapi_jetty;

import io.swagger.v3.jaxrs2.integration.OpenApiServlet;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.jersey.servlet.ServletContainer;

/**
 * https://anirtek.github.io/java/jetty/swagger/openapi/2021/06/12/Hooking-up-OpenAPI-with-Jetty.html
 *
 * http://localhost:8080/openapi
 * http://localhost:8080/api/openapi
 * http://localhost:8080/api/openapi.json  shows
 * {"openapi":"3.0.1","info":{"title":"MyTitle","description":"API Documentation","contact":{"url":"https://example.com/contact"},"licens
 *
 * http://localhost:8080/sampleapi/ping
 * Ping response
 *
 * http://localhost:8080
 * swagger UI
 */
public class Main {

    public static void main(String... args) throws Exception {
// Create and configure a ThreadPool.
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setName("server");

// Create a Server instance.
        Server server = new Server(threadPool);

// HTTP configuration and connection factory.
        HttpConfiguration httpConfig = new HttpConfiguration();
        HttpConnectionFactory http11 = new HttpConnectionFactory(httpConfig);

// Create a ServerConnector to accept connections from clients.
        ServerConnector connector = new ServerConnector(server, 1, 1, http11);
        connector.setPort(8080);
        connector.setHost("0.0.0.0");
        connector.setAcceptQueueSize(128);
        server.addConnector(connector);

// Create ContextHandlerCollection context
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        server.setHandler(contexts);

// Adding server path for API
        ContextHandler sampleApiHandler = new ContextHandler("/sampleapi");
        sampleApiHandler.setHandler(new SampleApiClass());
        contexts.addHandler(sampleApiHandler);

        // //

        // Setup Jetty Servlet
        ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        servletContextHandler.setContextPath("/");
        contexts.addHandler(servletContextHandler);

// Setup API resources to be intercepted by Jersey
        ServletHolder jersey = servletContextHandler.addServlet(ServletContainer.class, "/api/*");
        jersey.setInitOrder(1);
        jersey.setInitParameter(
                "jersey.config.server.provider.packages",
                "swagger_openapi_jetty;io.swagger.v3.jaxrs2.integration.resources");

        // //

        // Expose API definition independently into yaml/json
        ServletHolder openApi = servletContextHandler.addServlet(OpenApiServlet.class, "/openapi/*");
        openApi.setInitOrder(2);
        openApi.setInitParameter("openApi.configuration.resourcePackages", "swagger_openapi_jetty");

        // //

        // Setup Swagger-UI static resources
        String resourceBasePath = Main.class.getClassLoader().getResource("webapp").toExternalForm();
        servletContextHandler.setWelcomeFiles(new String[]{"index.html"});
        servletContextHandler.setResourceBase(resourceBasePath);
        servletContextHandler.addServlet(new ServletHolder(new DefaultServlet()), "/*");

        servletContextHandler.setInitParameter("javax.ws.rs.Application", SampleApiClass.class.getSimpleName());

        // //

        // Start the Server so it starts accepting connections from clients.
        server.start();
        server.join();
    }
}
