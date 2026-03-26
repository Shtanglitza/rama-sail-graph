package rama_sail;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.testsuite.sail.RDFStoreTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Var;

/**
 * RDF4J compliance test for RamaSail.
 *
 * Extends RDFStoreTest from rdf4j-sail-testsuite to verify that RamaSail
 * correctly implements the SAIL storage interface.
 *
 * The test manages a Rama in-process cluster (IPC) for the test lifecycle:
 * - BeforeAll: Start IPC, launch RdfStorageModule
 * - createSail: Return a new RamaSail connected to the module
 * - AfterAll: Close IPC
 */
public class RamaSailRDFStoreTest extends RDFStoreTest {

    private static Object ipc;
    private static String moduleName;
    private static IFn createRamaSail;

    @BeforeAll
    public static void setUpClass() {
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

            System.out.println("RamaSailRDFStoreTest: Started IPC with module " + moduleName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RamaSail test environment", e);
        }
    }

    @AfterAll
    public static void tearDownClass() {
        if (ipc != null) {
            System.out.println("RamaSailRDFStoreTest: Shutting down IPC");
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
        // Create a new RamaSail connected to the running module
        // Use sync-commits mode for tests to ensure data is visible immediately after commit
        //
        // Note: RDFStoreTest expects createSail() to return an already-initialized sail
        // See: "The returned repository should already have been initialized."
        Object options = Clojure.read("{:sync-commits true}");
        Sail sail = (Sail) createRamaSail.invoke(ipc, moduleName, options);
        sail.init();
        return sail;
    }

    /**
     * Override testAddWhileQuerying with a longer timeout.
     * Rama's IPC microbatch processing adds latency to concurrent add+query operations,
     * causing the default 60s JUnit timeout to be insufficient.
     */
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Override
    public void testAddWhileQuerying() {
        super.testAddWhileQuerying();
    }

    /**
     * Clear all data before each test to ensure test isolation.
     * Uses the connection created by super.setUp() to avoid stale handle issues.
     */
    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        try {
            con.begin();
            con.clear(new Resource[0]);
            con.commit();
        } catch (Exception e) {
            System.err.println("Warning: Failed to clear data: " + e.getMessage());
        }
    }
}
