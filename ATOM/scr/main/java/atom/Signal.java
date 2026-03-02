package atom;

public class Signal {
    public static void main(String[] args) {   
        try {
            Utils.sendStartSignal();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
