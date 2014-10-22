package org.aksw.linkedspending.job;

public class TestJob extends Job
{

	public TestJob()
	{
		super("orcamento_publico");
//		new Thread(new Runnable(){
//
//			@Override public void run()
//			{
//				try
//				{
//					System.out.println(json());
//					setState(State.RUNNING);
//					System.out.println(json());
//					Thread.sleep(10);
//					setState(State.PAUSED);
//					System.out.println(json());
//					Thread.sleep(10);
//					setState(State.STOPPED);
//					System.out.println(json());
//					Thread.sleep(10);
//					setState(State.CREATED); // should not work
//					System.out.println(json());
//				}
//				catch (InterruptedException e)	{e.printStackTrace();}
//
//			}
//		}).start();
	}

}