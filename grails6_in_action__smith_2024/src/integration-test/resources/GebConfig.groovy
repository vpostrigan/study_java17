import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.chrome.ChromeOptions
import org.openqa.selenium.firefox.FirefoxDriver

reportsDir = "target/geb-reports" // Directory to store screenshots and HTML snapshots

waiting {
    timeout = 5 // Default wait timeout in seconds
    retryInterval = 0.5 // How often to retry actions
}

environments {

    // run via “./gradlew -Dgeb.env=chrome iT”
    chrome {
        driver = { new ChromeDriver() }
    }

    // run via “./gradlew -Dgeb.env=chromeHeadless iT”
    chromeHeadless {
        driver = {
            ChromeOptions o = new ChromeOptions()
            o.addArguments('headless')
            new ChromeDriver(o)
        }
    }

    // run via “./gradlew -Dgeb.env=firefox iT”
    firefox {
        driver = { new FirefoxDriver() }
    }
}

// Set the default environment to use
// You can override this when running tests, e.g., `grails test-app functional -Dgeb.env=firefox`
geb.env = "chrome"

// Set the base URL of your Grails application
// This is typically the address where your Grails app runs during tests
baseUrl = "http://localhost:8080"
