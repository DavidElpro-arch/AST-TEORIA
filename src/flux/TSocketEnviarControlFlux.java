package flux;

import xarxa.Comms;
import xarxa.Segment;
import xarxa.TSocket;
import xarxa.Xarxa;



public class TSocketEnviarControlFlux extends TSocket {

    protected int seguentEnviar, seguentASerReconegut, finestraRecepcio;

    public TSocketEnviarControlFlux(Xarxa x) {
        super(x);
        finestraRecepcio = Comms.MIDA_CUA_RECEPCIO;
    }

    @Override
    public void enviar(Object c) {
        try {
            mon.lock();
            //Per enviar segments: xarxa.enviar(seg);
            while((seguentEnviar - seguentASerReconegut) >= finestraRecepcio){
            appCV.awaitUninterruptibly();
            }
            
            Segment seg = new Segment(Comms.DADES, seguentEnviar, c);
            xarxa.enviar(seg);
            seguentEnviar++;
           

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            mon.unlock();
        }
    }

    @Override
    public void processarMissatge(Segment segment) {
        if(segment == null) return;
        
        mon.lock();
        try {
           seguentASerReconegut = segment.getNumSeq();
           finestraRecepcio = segment.getFinestra();
           appCV.signalAll();
            

            
            System.out.println("ACK rebut -> " + "finestra recepcio: "
                    + finestraRecepcio);

        } finally {
            mon.unlock();
        }
    }
}
