package flux;

import ast.util.CircularQueue;
import xarxa.Comms;
import xarxa.Segment;
import xarxa.TSocket;
import xarxa.Xarxa;

public class TSocketRebreControlFlux extends TSocket {

    protected int seguentARebre;
    protected CircularQueue cuaRecepcio;

    public TSocketRebreControlFlux(Xarxa x) {
        super(x);
        cuaRecepcio = new CircularQueue(Comms.MIDA_CUA_RECEPCIO);
    }

    @Override
    public Object rebre() {
        mon.lock();
        try {
            while(cuaRecepcio.empty()){
                appCV.awaitUninterruptibly();
            }   
            
            return cuaRecepcio.get();
            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            mon.unlock();
        }
    }

    @Override
    public void processarMissatge(Segment segment) {
        
        if(segment == null) return;
        
        mon.lock();
        try {
           cuaRecepcio.put(segment.getDades());
           appCV.signalAll();
           seguentARebre ++;
           
           int espaiLLiure = cuaRecepcio.free();
           Segment ack = new Segment(Comms.ACK, seguentARebre, espaiLLiure, null);
           xarxa.enviar(ack);
            
            
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mon.unlock();
        }
    }
}
