package org.apache.spark.instrument

object SampleEvents {
  val good = Map(
    "simple" -> "name(arg1, arg2)",
    "typical" -> "(03b18074-af72-4104-9244-f16c5d1f42da,1506444010559,SpanStart(ff926f1a-10a7-4e18-88f6-c0491cbf1774,Fn(org.apache.spark.rpc.RpcEnv$.create(java.lang.String,java.lang.String,java.lang.String,int,org.apache.spark.SparkConf,org.apache.spark.SecurityManager,int,boolean),ArrayBuffer(sparkExecutor,localhost.localdomain, localhost.localdomain, -1, org.apache.spark.SparkConf@51d83274,org.apache.spark.SecurityManager@4c165bca, 1, true),org.apache.spark.rpc.netty.NettyRpcEnv@413afd06)))",
    "shallow" -> "q",
    "deep" -> "a(b(c(d(e(f,g(h(i),j,k,l(m)),n,o,p)),q(r(s(t,u),v,w))),x,y),z,A,B,C(D,E,F,G,H,I,J,K,L,M,N,O(P,Q(R),S),T,U,V,W),X(Y),Z)",
    "parens" -> "(((),(),((),(()),(),()),((((((),(()),(),(),())))),(),()),(((()))),((),())))",
    "special" -> """^*&(}}],\]|(),$%~!#@@@(<>(),$%^''',<""("),*&^('.?,???|)),-=_+_[(?```#!,]]|}(),:,;;%;[),{}{.}}}*p[[(;.'_,'\',\\\>~,~,@!,:;'(&',\`~@())))""",
    "whitespace" -> "  \t a(\t\t b(  c ,  \r d\t(  e, \n f\r\t , g(h,i)\t, \f\f\t j \n), k \n\n\n), l \f, m\t\n\t\t\t)  \t\r\n",
    "empty" -> "(,,a,,,b(c,),d(),,(,,),,)",
    "nested" -> "((((((a))))))",
    "nothing" -> ""
  )

  val bad = Map(
    "mismatch" -> "a(b(c,d),e()",
    "unenclosed" -> "a(b,c),d,e",
    "missingComma1" -> "a(b,c(d,e)f(g),h)",
    "missingComma2" -> "a(b(c)(d))",
    "single" -> ")",
    "comma" -> ","
  )

  val services = Seq(
    Seq(
      "(29011d0b-a761-497e-82cf-b4b7d4f65986,1506959575499,Service(driverPropsFetcher,/10.50.108.70:41222))",
      "(29011d0b-a761-497e-82cf-b4b7d4f65986,1506959575617,Service(sparkExecutor,/10.50.108.70:41224))",
      "(3bcf41dd-fcb9-40f4-9ce0-da5337598517,1506959575462,Service(sparkDriver,/10.50.108.70:35766))",
      "(77152954-d105-46ca-b6cf-3a32c76e42a6,1506959578006,Service(driverPropsFetcher,/10.50.108.70:41246))",
      "(77152954-d105-46ca-b6cf-3a32c76e42a6,1506959578146,Service(sparkExecutor,/10.50.108.70:41250))",
      "(d2e232aa-9e20-426e-8c72-749d2124c719,1506959578004,Service(driverPropsFetcher,/10.50.108.70:41244))",
      "(d2e232aa-9e20-426e-8c72-749d2124c719,1506959578145,Service(sparkExecutor,/10.50.108.70:41248))"
    ),
    Seq(
      "(3fa75892-2e53-44d0-9a5e-fed7750f1c31,1505917128263,Service(driverPropsFetcher,/10.50.108.70:44848))",
      "(3fa75892-2e53-44d0-9a5e-fed7750f1c31,1505917128359,Service(sparkExecutor,/10.50.108.70:44850))",
      "(6561ca3d-dfd1-4596-9295-b9fa77aec480,1505917124904,Service(driverPropsFetcher,/10.50.108.70:44834))",
      "(6561ca3d-dfd1-4596-9295-b9fa77aec480,1505917125054,Service(sparkExecutor,/10.50.108.70:44836))",
      "(8360fd37-8cb5-47a9-a8dc-e12f32ce89a7,1505917121978,Service(sparkYarnAM,/10.50.108.70:44826))",
      "(af5470e0-4dfe-4e8e-ba92-59efd4266acb,1505917126483,Service(driverPropsFetcher,/10.50.108.70:44842))",
      "(af5470e0-4dfe-4e8e-ba92-59efd4266acb,1505917126622,Service(sparkExecutor,/10.50.108.70:44844))",
      "(b5b55c94-21bc-4f90-a027-39c4591cc65f,1505917121956,Service(sparkDriver,/10.50.108.70:38638))"
    )
  )

  val dagSchedEv = "org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive"
  val blockUpdEv = "org.apache.spark.storage.BlockManagerMaster.updateBlockInfo"

  val run = Seq(
    "(a,1,SpanStart(d,JVMStart))",
    "(a,4,SpanEnd(d))",
    "(a,5,Service(sparkDriver,/1:1))",
    "(a,15,RPC(/2:1,/1:1,Response))",
    s"(a,16,Fn($dagSchedEv,(ExecutorAdded(Exec 1)),null))",
    s"(a,20,Fn($dagSchedEv,(JobSubmitted(Job 1)),null))",
    s"(a,22,Fn($dagSchedEv,(BeginEvent(Task(1,1))),null))",
    s"(a,32,Fn($dagSchedEv,(CompletionEvent(Task(1,1))),null))",
    s"(a,34,Fn($dagSchedEv,(BeginEvent(Task(1,2))),null))",
    s"(a,40,Fn($dagSchedEv,(CompletionEvent(Task(1,2))),null))",
    "(a,48,MainEnd)",
    "(a,50,InstrumentOverhead(2))",

    "(b,2,SpanStart(e,JVMStart))",
    "(b,4,SpanEnd(e))",
    "(b,8,Service(sparkExecutor,/2:1))",
    "(b,14,RPC(/1:1,/2:1,Request))",
    s"(b,24,Fn($blockUpdEv,(B,X,Y)))",
    s"(b,28,Fn($blockUpdEv,(B,X,Z)))",
    "(b,30,SpanStart(f,Fn(add(int,int),(1,2),3)))",
    "(b,31,SpanEnd(f))",
    "(b,42,MainEnd)",
    "(b,43,InstrumentOverhead(4))",

    "(c,1,SpanStart(g,JVMStart))",
    "(c,2,SpanEnd(g))",
    "(c,3,Service(filterMe,/3:1))",
    "(c,4,Fn(name(jvm,id),(C,1)))",
    "(c,100,MainEnd)"
  )

  val time = Seq(
    "(x,1,a,start)",
    "(x,10,a,end)",
    "(x,8,b,end)",
    "(x,3,b,start)",
    "(x,6,c,start)",
    "(x,4,c,end)",

    "(x,2,SpanStart(a,get))",
    "(x,4,SpanEnd(a,get))",
    "(x,9,SpanEnd(b,get))",
    "(x,6,SpanStart(b,get))",
    "(x,10,SpanStart(c,get))",
    "(x,9,SpanEnd(c,get))",
    "(x,4,SpanStart(d,ignore))",
    "(x,12,SpanEnd(d,ignore))"
  )
}
