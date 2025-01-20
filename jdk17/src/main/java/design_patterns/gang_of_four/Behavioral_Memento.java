package design_patterns.gang_of_four;

public class Behavioral_Memento {

    public class SiteDescrBean {
        private long id;
        private String name;
        private String theURL;
        private String description;
        private int category;

        private Memento undo;

        private class Memento {
            String siteDescr;
            int siteCateg;

            Memento(String descr, int category) {
                siteDescr = descr;
                siteCateg = category;
            }

            String getDescr() {
                return siteDescr;
            }

            int getCateg() {
                return siteCateg;
            }
        }

        public void preview() {
            // ...
            undo = new Memento(description, category);
            // ...
        }

        public void undoChanges() {
            description = undo.getDescr();
            category = undo.getCateg();
            // ...
        }
    }

}
