/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package estats;

import xarxa.Xarxa;


/**
                              +-------------+
                              |  ESTAT_TANC |------------
                              +-------------+             \
                         espera()    |                     \
                        ---------    |                      |   inicia()
                                     V                      | ------------
                               +------------+               | envia INICI
                               |  ESTAT_ESP |               |
                               +------------+         +-----------------+
                                     |                | ESTAT_ENV_INICI |
                                     |                +-----------------+
                                     |                      |           
                 reb INICI           |                      |reb RECON                                   
              -------------------    |                      |----------------       
               envia RECON           |                      |  
               envia INICI           |                      |     
                                     |                      | 
                           +-----------------+        +-------------------+
                           | ESTAT_ESP_RECON |        | ESTAT_REBUT_RECON |
                           +-----------------+        +-------------------+ 
                                     |                      |
                  reb RECON          |                      |reb INICI           
                 -----------         |                      |-----------------
                                     |                      |envia RECON
                                     V                      /
                              +--------------+             /
                              |  ESTAT_FINAL |<------------
                              +--------------+
 */
public class TSocketCanviEstats2 extends TSocketCanviEstats{
    
    public TSocketCanviEstats2(Xarxa x)  {
        super(x);
        estat = ESTAT_TANC;
    }


   
    public void inicia(){
        mon.lock();
        try{
           while(estat != ESTAT_TANC){
           appCV.awaitUninterruptibly();
           
           }
           
           xarxa.enviar(INICI);
           estat = ESTAT_ENV_INICI;
            
           while(estat != ESTAT_FINAL){
           appCV.awaitUninterruptibly();
           
           }
            
                      
            
            
            
            
        } catch (Exception ex) { System.out.println(ex); }
        finally{
            mon.unlock();
        }
    }
    

    public void espera(){
        mon.lock();
        try{
            while(estat != ESTAT_TANC){
            appCV.awaitUninterruptibly();
            
            }
            estat = ESTAT_ESP;
            
            while(estat != ESTAT_FINAL){
           appCV.awaitUninterruptibly();
           
           }
            
            
            
        } catch (Exception ex) { System.out.println(ex); }
        finally{
            mon.unlock();
        }
    }   

    public void processarMissatge(Object miss){
        mon.lock();
        try {
          switch(estat){
              case ESTAT_ESP: {
                  if(miss.equals(INICI)){
                      xarxa.enviar(RECON);
                      xarxa.enviar(INICI);
                      estat = ESTAT_ESP_RECON;
                  
                  }
                  break;
              }
              
              case ESTAT_ENV_INICI: {
              if(miss.equals(RECON)){
                  estat = ESTAT_REBUT_RECON;
              }
              break;
              }
              
              case ESTAT_REBUT_RECON: {
                  if(miss.equals(INICI)){
                      xarxa.enviar(RECON);
                      estat = ESTAT_FINAL;
                      appCV.signalAll();
                  }
                  break;
              }
              
              case ESTAT_ESP_RECON: {
                 if (miss.equals(RECON)) {
                    estat = ESTAT_FINAL;
                    appCV.signalAll();
               }
                break;
                }
          
          
          }
        } finally {
            mon.unlock();
        }
    }
}
