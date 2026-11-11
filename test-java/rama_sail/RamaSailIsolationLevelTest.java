package rama_sail;

import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.testsuite.sail.SailIsolationLevelTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Var;

/**
 * RDF4J isolation-level test for RamaSail.
 *
 * SailIsolationLevelTest runs its checks for every isolation level the Sail
 * reports as supported, so this verifies that the levels RamaSail advertises
 * are the ones it provides.
 */
public class RamaSailIsolationLevelTest extends SailIsolationLevelTest {

    private static Object ipc;
    private static String moduleName;
    private static IFn createRamaSail;

    @BeforeAll
    public static void startCluster() {
        try {
            // Load required Clojure namespaces
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("rama-sail.core"));
            require.invoke(Clojure.read("rama-sail.sail.adapter"));
            require.invoke(Clojure.read("com.rpl.rama"));
            require.invoke(Clojure.read("com.rpl.rama.test"));

            // Get functions
            IFn createIpc = Clojure.var("com.rpl.rama.test", "create-ipc");
            IFn launchModule = Clojure.var("com.rpl.rama.test", "launch-module!");
            IFn getModuleName = Clojure.var("com.rpl.rama", "get-module-name");
            createRamaSail = Clojure.var("rama-sail.sail.adapter", "create-rama-sail");

            // Get the module class - it's a Var, get its value
            Var moduleVar = (Var) Clojure.var("rama-sail.core", "RdfStorageModule");
            Object rdfStorageModule = moduleVar.deref();

            // Create IPC and launch module
            ipc = createIpc.invoke();

            // Launch with {:tasks 4 :threads 2}
            Object options = Clojure.read("{:tasks 4 :threads 2}");
            launchModule.invoke(ipc, rdfStorageModule, options);

            // Get module name
            moduleName = (String) getModuleName.invoke(rdfStorageModule);

            System.out.println("RamaSailIsolationLevelTest: Started IPC with module " + moduleName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RamaSail test environment", e);
        }
    }

    @AfterAll
    public static void stopCluster() {
        if (ipc != null) {
            System.out.println("RamaSailIsolationLevelTest: Shutting down IPC");
            try {
                ((AutoCloseable) ipc).close();
            } catch (Exception e) {
                System.err.println("Error closing IPC: " + e.getMessage());
            }
            ipc = null;
        }
    }

    @Override
    protected Sail createSail() {
        // sync-commits: a commit returns once its data is visible, as the suite expects
        Object options = Clojure.read("{:sync-commits true}");
        return (Sail) createRamaSail.invoke(ipc, moduleName, options);
    }
}
