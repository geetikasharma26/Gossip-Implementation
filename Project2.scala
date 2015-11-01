
import scala.math._
import scala.util.Random
import akka.actor.Actor
import akka.actor.Props
import akka.actor._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.control.Breaks._

case class Gossip()
case class InitiateActor(actorId : Int ,allActors : List[ActorRef] ,neighbor :List[Int] , statusKeep :ActorRef )
case class ActorStatus(playerId :Int , messagecount :Int)
case class InitStatus(system:ActorSystem , nodeCount:Int ,algo :String)
case class PushSumGossip(sum:Double, weight:Double)
case class PushSumCountList(memberId:Int)

object bitcoin extends App
 { override def main (Args:Array[String]) 
  { if ( Args.length != 3 )
    { println (" Please pass correct arguments")
      System.exit(1)
    }
 
    var NumNodes :Int    = Args(0).toInt
    var topology :String = Args(1)
    var algo     :String = Args(2)
    
    val system = ActorSystem("GossipPushSum")
    var allActors: List[ActorRef] = Nil
    var master:ActorRef = system.actorOf(Props[Master])
    var i: Int = 0
     
    while (i<NumNodes)
    {allActors ::= system.actorOf(Props[Actors])   
      i += 1
    }  
  
  // If Topology is full
  if ("full".equalsIgnoreCase(topology))
  { 
     var i : Int = 0
       
     while (i< allActors.length)
     { 
       var  neighbours :List[Int] = Nil
       var j:Int = 0
       
       while (j< allActors.length)
       { if (i!=j)
         { neighbours ::= j
         }
         j+=1
       }
       allActors(i)!InitiateActor(i,allActors,neighbours,master)
       i+=1
     } 
  
  } 
  
   var size = 0
   // If Topology is 2D and imp2D
   if(topology=="2D" || topology=="imp2D") 
   { 
     size = (scala.math.ceil(scala.math.sqrt(NumNodes))).toInt;
   }
   
   
  // If Topology is 2D
  if("2D".equalsIgnoreCase(topology)){
          var i:Int = 0
          println()
          while(i<allActors.length){
            var neighbors:List[Int] = Nil
            
          // not top row
            if (i+1 > size){
              neighbors ::=i-size
            }
            //not bottom row
            if (i+1+size <= NumNodes){
              neighbors ::=i+size
            } 
            //not left most column
            if ((i+1)%size != 1 ){
              neighbors ::=i-1 
            }
            //not right most column
            if ((i+1)%size != 0){
              if(i+1<NumNodes){
                neighbors ::=i+1
              }
            }
           allActors(i) ! InitiateActor(i, allActors, neighbors, master)            
            i += 1
          }
                   
        }
  
  
        if("line".equalsIgnoreCase(topology))
        {
          var i:Int = 0;
          while(i<allActors.length)
          {
            var neighbors:List[Int] = Nil
            
            if(i>0) neighbors ::= (i-1)
            if(i<allActors.length-1) neighbors ::= (i+1)
            
            allActors(i) ! InitiateActor(i, allActors, neighbors, master)
            
            i += 1
          }
        }
        
        if("imp2D".equalsIgnoreCase(topology)){
          var i:Int = 0
          while(i<allActors.length){
            var neighbors:List[Int] = Nil
            var tempList:List[Int] = Nil
            
            // not top row
            if (i+1 > size){
              neighbors ::=i-size
            }
            //not bottom row
            if (i+1+size <= NumNodes){
              neighbors ::=i+size
            } 
            //not left most column
            if ((i+1)%size != 1 ){
              neighbors ::=i-1 
            }
            //not right most column
            if ((i+1)%size != 0){
              if(i+1<NumNodes){
                neighbors ::=i+1
              }
            }
            
            var random:Int = -1;
            do{
              random = Random.nextInt(allActors.length)
              for(x<-tempList){
                if(random == x) random = -1
              }   
            }while(random == -1)
            
            neighbors ::= (random)
            allActors(i) ! InitiateActor(i, allActors, neighbors, master)
            
            i += 1
          }
        }
  
   master ! InitStatus(system, NumNodes , algo)
  
  if ("gossip".equalsIgnoreCase(algo))
  { 
    allActors(0)!Gossip    
  }
  
    if("pushsum".equalsIgnoreCase(algo) )
    {
      println("starting push sum")
      var randomMember = Random.nextInt(allActors.length)
      println("Selected a starting actor")
      allActors(randomMember) ! PushSumGossip(0,1)
          }
    
 }//End of main loop
 
}//End of gossip loop


class Master extends Actor 
{   var time:Long = 0;
    var NumNodes:Int = 0;
    var maxLimit:Int = 0;
    var keeperSys:ActorSystem = null;
    var shouldWork:Boolean = false;
    var stableNodeCount:Int = 0;
    
    println("Status Keeper Started ...")
    time = System.currentTimeMillis
    println("Start Time: "+time)
    var statusList:List[ActorStatus] = Nil
    var i : Int = 0
    var statusListPushSum:List[PushSumCountList] = Nil

    def receive = 
        {
        case InitStatus(sys:ActorSystem, nodeCount:Int , algo:String) => 
          {
          time = System.currentTimeMillis
          keeperSys = sys;
          NumNodes = nodeCount;
          maxLimit = 10;
          if(algo.equalsIgnoreCase("gossip")) shouldWork = true;
          }           
          
        case ActorStatus(actorId:Int , msgcount :Int) =>
          { 
          var tempStatusList:List[ActorStatus] = Nil
          var i:Int = 0
          while(i<statusList.length){
            
            if(statusList(i).playerId!=actorId) tempStatusList ::= statusList(i) 
            i += 1
          }
          statusList = tempStatusList
          statusList = statusList ::: List(new ActorStatus(actorId, msgcount))       
        
          i = 0;
          breakable
          {
        	  while (i<statusList.length)
        	  {
        		  if (!(statusList(i).messagecount  > 9))
        			  break
        	      else
        	    	  i += 1
        	  }
          }
          
          if (i == statusList.length)
          { println("The time taken to converge for Gossip is  " + (System.currentTimeMillis-time) +" milli seconds for " + NumNodes + " nodes")
            println("The system is shutting down")
            System.exit(1)
          }       
         }          
          
        case PushSumCountList(memberId:Int) => {
          var foundentry:Boolean = false;
            breakable {
              for (i<- 0 until statusListPushSum.length){
                if(statusListPushSum(i).memberId == memberId){
                  foundentry =true
                  break;
                }
              }
          }
            if(! foundentry){
              statusListPushSum = statusListPushSum ::: List(new PushSumCountList(memberId))
          }

            if((statusListPushSum.length.toDouble/NumNodes.toDouble) == 1)
            {
                println("The time taken to converge for GossipPushSum is  " + (System.currentTimeMillis-time) +" for" + NumNodes + " nodes")
                println("The system is shutting down")
                System.exit(1)
            }
        }
  
   }//End of Def
}//End of class master

 case class Actors() extends Actor
{     var maxlimit:Int = 10
      var playerId:Int = 0
      var allPlayers:List[ActorRef] = Nil    
      var Master:ActorRef = null
      var neighbors:List[Int] = Nil
      var messagecount:Int = 0
      var msgrep:Int       = 0
      var rumorCounter:Int = 0
      var stabilityCounter:Int = 0;
      var s:Double = 0
      var w:Double = 0
      var isStable:Boolean = false;
        
  def receive = 
  { case InitiateActor(actorId:Int , allNodes:List[ActorRef], neighborList:List[Int] , statusKeep:ActorRef) =>
    { 
          neighbors = neighbors ::: neighborList
          playerId = actorId
          Master = statusKeep
          allPlayers = allNodes 
    }
  
  
    case Gossip =>
      {       
       if (messagecount < maxlimit)
        { messagecount+=  1
          println("Received ping to ID:  "+playerId+"  Counter -- "+messagecount)
        
        
          Master! ActorStatus (playerId , messagecount)
       
          var randomPlayer = 0
          msgrep = allPlayers.length
 
          for (c<- 0 until msgrep)
          { randomPlayer = Random.nextInt(neighbors.length)
            allPlayers(neighbors(randomPlayer)) ! Gossip
          }
       
      } 
         else
         {
             
              var randomMember = Random.nextInt(neighbors.length);
              allPlayers(neighbors(randomMember)) ! PushSumGossip(s,w)
         }   
       
      }//End of case gossip
      
    case PushSumGossip(sum:Double, weight:Double) => 
    {
          
          if(!isStable)
          {
              println("MemberId: "+playerId+"  Receives S/W: "+(sum/weight)+" -- mine s/w: "+(s/w));
              rumorCounter += 1;
              var oldSbyW:Double = s/w;
              s+=sum;
              w+=weight;
              s = s/2;
              w = w/2;
              var newSbyW:Double = s/w;
              println("Inside PushSum ---- oldSbyW: "+oldSbyW+"   --- newSbyW: "+newSbyW);
          
              if(rumorCounter==1 || Math.abs((oldSbyW-newSbyW))>math.pow(10, -10)) {
              stabilityCounter=0;
              println("From Actor: "+playerId+"\tSending Message ... Sum: "+s+" Weight: "+w);
              var randomPropogator = Random.nextInt(neighbors.length);
              allPlayers(neighbors(randomPropogator)) ! PushSumGossip(s,w)
          }
          else
          {
              stabilityCounter+=1;
              if(stabilityCounter>3) 
              {
              println("Stability condition  "+playerId+" \tRumor Count  "+rumorCounter+" \ts/w: "+(s/w));
              Master ! PushSumCountList(playerId); 
              isStable = true
              var randomMember = Random.nextInt(neighbors.length);
              allPlayers(neighbors(randomMember)) ! PushSumGossip(s,w)
              }
              else
              {
               println("From Actor: "+playerId+" Sending Message ... Sum: "+s+" Weight: "+w);
               var randomMember = Random.nextInt(neighbors.length);
               allPlayers(neighbors(randomMember)) ! PushSumGossip(s,w)
              }
          }
         }
     
              else{
              var randomMember = Random.nextInt(neighbors.length);
              allPlayers(neighbors(randomMember)) ! PushSumGossip(s,w)
      }   
  }   

}
}