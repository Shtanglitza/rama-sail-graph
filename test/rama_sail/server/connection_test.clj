(ns rama-sail.server.connection-test
  "Unit tests (no IPC) for the server's connection state helpers."
  (:require [clojure.test :refer [deftest is testing]]
            [rama-sail.server.connection :as conn]))

(deftest test-stop-closes-every-resource
  (testing "stop! closes an AutoCloseable connection (a RepositoryConnection is not java.io.Closeable)"
    (let [closed (atom [])]
      (conn/stop! {:conn (reify java.lang.AutoCloseable (close [_] (swap! closed conj :conn)))
                   :ipc (reify java.io.Closeable (close [_] (swap! closed conj :ipc)))})
      (is (= [:conn :ipc] @closed)))))
