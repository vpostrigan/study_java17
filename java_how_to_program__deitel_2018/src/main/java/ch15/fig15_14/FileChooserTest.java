package ch15.fig15_14;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

// Fig. 15.13: FileChooserTest.java
// App to test classes FileChooser and DirectoryChooser.
public class FileChooserTest extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("FileChooserTest.fxml"));

        Scene scene = new Scene(root);
        stage.setTitle("File Chooser Test"); // displayed in title bar
        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }

}
