package ch13.Painter;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

// Fig. 13.5: Painter.java
// Main application class that loads and displays the Painter's GUI.
public class Painter extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("Painter.fxml"));

        Scene scene = new Scene(root);
        stage.setTitle("Painter"); // displayed in window's title bar
        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }

}
