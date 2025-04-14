package ch08.GUIGraphicsCaseStudy08;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

// DrawRandomLines.java
// Main application class that loads and displays the DrawRandomLines GUI.
public class DrawRandomLines extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        // loads Welcome.fxml and configures the DrawRandomLinesController
        Parent root = FXMLLoader.load(getClass().getResource("DrawRandomLines.fxml"));

        Scene scene = new Scene(root); // attach scene graph to scene
        stage.setTitle("Draw Random Lines"); // displayed in window's title bar
        stage.setScene(scene); // attach scene to stage
        stage.show(); // display the stage
    }

    public static void main(String[] args) {
        launch(args);
    }

}
