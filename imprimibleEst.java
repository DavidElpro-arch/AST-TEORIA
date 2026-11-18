public interface Comms {
    public final static String HOST = "localhost";
    public final static int PORT = 2222;
    public final static int MIDA_CUA = 2;

    public final static int DADES = 100;
    public final static int ACK = 200;
}

public class Generador implements Runnable {

    protected int maxim;
    protected ProtocolEnviar prot;

    public Generador(int max) {
        maxim = max;
        prot = new ProtocolEnviar();
    }

    public void run() {
        int num = 0;
        while (num < maxim) {
            prot.enviar(new Integer(num));
            num = num + 1;
        }
    }
}

public class Servidor implements Runnable {

    protected ServerSocket ss;
    protected ProtocolRebre prot;

    public Servidor() {
        try {
            ss = new ServerSocket(Comms.PORT);
        } catch (IOException ex) { }
    }

    public void run() {
        Socket s = null;
        try {
            s = ss.accept();
        } catch (IOException ex) { }
        prot = new ProtocolRebre(s);
        while (true) {
            int num = prot.rebre();
            System.out.println("Servidor: " + num);
            adormir(num);
        }
    }

    private void adormir(int t) {
        try {
            Thread.sleep(1000 * t);
        } catch (InterruptedException ex) {
            Logger.getLogger(Protocol.class.getName()).log(Level.SEVERE,
                    null, ex);
        }
    }
}



















public abstract class Protocol{
    protected Socket sc;
    protected ObjectInputStream entrada;
    protected ObjectOutputStream sortida;
    protected ReentrantLock mon;
    protected Condition appCV;

    public Protocol(Socket s) {
        try {
            sc = s;
            entrada = new ObjectInputStream(sc.getInputStream());            
            sortida = new ObjectOutputStream(sc.getOutputStream());

            mon = new ReentrantLock();
            appCV = mon.newCondition();
            new Thread(new PartReceptora()).start();
        } catch (IOException ex) { }
    }

    public Protocol() {
        try {
            sc = new Socket(Comms.HOST, Comms.PORT);
            
            sortida = new ObjectOutputStream(sc.getOutputStream());
            entrada = new ObjectInputStream(sc.getInputStream());
            mon = new ReentrantLock();
            appCV = mon.newCondition();
            new Thread(new PartReceptora()).start();
        } catch (IOException ex) { }
    }

    public void close() {
        try {
            entrada.close();
            sortida.close();
            sc.close();
        } catch (IOException ex) { }
    }

    public abstract void processarMissatge(Object miss);

    class PartReceptora implements Runnable {

        public void run() {
            while (true) {
                Object rebut = null;
                try {
                  
                    rebut = entrada.readObject();
                 
                } 
                catch (ClassNotFoundException ex) {
                    Logger.getLogger(Protocol.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(Protocol.class.getName()).log(Level.SEVERE, null, ex);
                }
                processarMissatge(rebut);
            }
        }
    }
}


















public class ProtocolEnviar extends Protocol {

    protected boolean noPucEnviar;

    public void enviar(Integer i) {
        try {










        } catch (Exception ex) { } 
        finally {
            mon.unlock();
        }
    }

    public void processarMissatge(Object resposta) {
        mon.lock();



        mon.unlock();
    }
}

public class ProtocolRebre extends Protocol {

    protected Integer dadesRebudes;

    public ProtocolRebre(Socket s) {
        super(s);
    }

    public int rebre() {
        mon.lock();












        mon.unlock();


    }

    public void processarMissatge(Object missatge) {
        mon.lock();




        mon.unlock();
    }
}








/*
 * Ara el servidor es despista i de tant en tant perd missatges.
 * Programeu el protocol stop and wait en aquest cas
 *
 */

class ProtocolEnviarDespistat extends Protocol {

    protected static final int SND_RTO = 500; // Retransmission timeout in milliseconds
    protected Timer timer;
    protected Timer.Task tascaTimer;

    protected Integer missatgeNoReconegut;
    /* missatgeNoReconegut es null quan
     * quan s'ha reconegut l'ultim missatge enviat
     *
     */

    public ProtocolEnviarDespistat() {
        super();
        timer = new Timer();
    }

    public void enviar(Integer i) {
        try {
            mon.lock();










        } catch (Exception ex) { } 
        finally {
            mon.unlock();
        }
    }

    /**
     * Timeout elapsed.
     */
    protected void timeout() {
        mon.lock();
        try {








        } finally {
            mon.unlock();
        }
    }

    protected void startRTO() {
        if (tascaTimer != null) {
            tascaTimer.cancel();
        }
        tascaTimer = timer.startAfter(
                new Runnable() {
                    @Override
                    public void run() {
                        timeout();
                    }
                },
                SND_RTO, TimeUnit.MILLISECONDS);
    }
    
    
    
    


    protected void stopRTO() {
        if (tascaTimer != null) {
            tascaTimer.cancel();
        }
        tascaTimer = null;
    }

    public void processarMissatge(Object resposta) {
        mon.lock();






        mon.unlock();
    }
}


class ProtocolRebreDespistat extends Protocol {
    protected Integer dadesRebudes;
    // dadesRebudes es null quan no hi ha dades rebudes
    
    public ProtocolRebreDespistat(Socket s) {
        super(s);
    }
    
    public int rebre() {
        mon.lock();













        mon.unlock();



    }

    public void processarMissatge(Object missatge) {
        mon.lock();
        try {
            double llindar = Math.random();
            if (llindar > 0.6) {
                System.out.println("Servidor -> dades perdudes : " + missatge);
                return;
            }
            dadesRebudes = (Integer) missatge;
            appCV.signalAll();
        } finally {
            mon.unlock();
        }
    }
}















/*
 * Ara tambe es despista l'emissor, per tant
 * el servidor de tant en tant perd missatges,
 * i l'emissor de tant en tant perd ACKs
 * Programeu el protocol stop and wait en aquest cas
 *
 */


 public class ProtocolEnviarPerdACK  extends Protocol{

    protected static final int SND_RTO = 500; // Retransmission timeout in milliseconds
    protected Timer timer;
    protected Timer.Task tascaTimer;

    protected Missatge missatgeNoReconegut;
    // missatgeNoReconegut es null quan 
    // l'ultim missatge ha sigut reconegut
    protected int sndNext;
    // Que indica rcvNext i perque es necessari?
    
    public ProtocolEnviarPerdACK() {
        super();
        timer = new Timer();
    }
  
    public void enviar(Integer i) {
        try {
            mon.lock();















        } catch (Exception ex) { } 
        finally {
            mon.unlock();
        }
    }

    /**
     * Timeout expira
     */
    protected void timeout() {
        mon.lock();
        try {
            if (missatgeNoReconegut != null) {
                try {







                } catch (IOException ex) { }
            }
        } finally {
            mon.unlock();
        }
    }

    protected void startRTO() {
        if (tascaTimer != null) {
            tascaTimer.cancel();
        }
        tascaTimer = timer.startAfter(
                new Runnable() {
                    @Override
                    public void run() {
                        timeout();
                    }
                },
                SND_RTO, TimeUnit.MILLISECONDS);
    }

    protected void stopRTO() {
        if (tascaTimer != null) {
            tascaTimer.cancel();
        }
        tascaTimer = null;
    }

    public void processarMissatge(Object resposta) {
        mon.lock();
        try {













 
        } catch (IOException ex) {
            Logger.getLogger(ProtocolEnviarPerdACK.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            mon.unlock();
        }
    }
}



public class ProtocolRebrePerdACK extends Protocol{
    protected Missatge dadesRebudes;
    // dadesRebudes es null quan no hi ha dades rebudes

    protected int rcvNext;
    // Que indica rcvNext i perque es necessari?
    
    public ProtocolRebrePerdACK(Socket s) {
        super(s);
    }
    
    public int rebre() {
        mon.lock();
















        mon.unlock();


    }
    

    public void processarMissatge(Object missatge) {
        mon.lock();
        try {
            double llindar = Math.random();
            if (llindar > 0.8) {
                System.out.println("Servidor -> dades perdudes : " + (Missatge)missatge);
                return;
            }
            if (((Missatge) missatge).getNumSeq() != rcvNext) {
                Missatge ack = new Missatge(Comms.ACK, rcvNext, 0);
                sortida.writeObject(ack);
                sortida.flush();
                return;
            }
            if (dadesRebudes != null && !(dadesRebudes.equals((Missatge) missatge))) {
                System.out.println("Dades rebudes i queden per consumir: Aixo no ha de passar!! "+dadesRebudes);
                return;
            }
            
            dadesRebudes = (Missatge) missatge;
            appCV.signalAll();

        } catch (Exception e) {
        } finally {
            mon.unlock();
        }
    }
}




