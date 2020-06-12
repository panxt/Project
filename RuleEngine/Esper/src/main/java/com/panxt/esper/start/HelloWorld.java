package com.panxt.esper.start;

/*import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;*/
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import com.panxt.esper.bean.Apple;
//import com.panxt.esper.bean.AppleListener;
import com.panxt.esper.bean.PersonEvent;
import org.junit.Test;


/**
 * @author panxt
 */
public class HelloWorld {

/*    @Test
    public void testApple() {
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();

        EPAdministrator admin = epService.getEPAdministrator();

        String product = Apple.class.getName();
        System.out.println(product);
        String epl = "select avg(price) from " + product + ".win:length_batch(3)";
//        String epl = "select avg(price) from Apple.win:length_batch(3)";

        EPStatement state = admin.createEPL(epl);
        state.addListener(new AppleListener());

        EPRuntime runtime = epService.getEPRuntime();

        Apple apple1 = new Apple();
        apple1.setId(1);
        apple1.setPrice(5);
        runtime.sendEvent(apple1);

        Apple apple2 = new Apple();
        apple2.setId(2);
        apple2.setPrice(2);
        runtime.sendEvent(apple2);

        Apple apple3 = new Apple();
        apple3.setId(3);
        apple3.setPrice(5);
        runtime.sendEvent(apple3);
    }

    @Test
    public void testPerson() {
        EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider();

        engine.getEPAdministrator().getConfiguration().addEventType(PersonEvent.class);
        String epl = "select name, age from PersonEvent";
        EPStatement statement = engine.getEPAdministrator().createEPL(epl);

        statement.addListener( (newData, oldData) -> {
            String name = (String) newData[0].get("name");
            int age = (int) newData[0].get("age");
            System.out.println(String.format("Name: %s, Age: %d", name, age));
        });

        engine.getEPRuntime().sendEvent(new PersonEvent("Peter", 10));


    }*/

    @Test
    public void testPerson(){
        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(PersonEvent.class);


        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments args = new CompilerArguments(configuration);
        EPCompiled epCompiled;
        try {
            epCompiled = compiler.compile("@name('my-statement') select name, age from PersonEvent", args);
        }
        catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        }
        catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "my-statement");

        statement.addListener( (newData, oldData, statement1, runtime1) -> {
            String name = (String) newData[0].get("name");
            int age = (int) newData[0].get("age");
            System.out.println(String.format("Name: %s, Age: %d", name, age));
        });

        runtime.getEventService().sendEventBean(new PersonEvent("Peter", 10), "PersonEvent");

    }

}





