package io.mosip.commons.khazana.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class ObjectStoreUtilTest {

    @Test
    void getName_sourceProcessObject_shouldComposeCorrectly() {
        // Arrange
        String source = "src";
        String process = "proc";
        String objectName = "obj";

        // Act
        String name = ObjectStoreUtil.getName(source, process, objectName);

        // Assert
        assertEquals("src/proc/obj", name);
    }

    @Test
    void getName_containerSourceProcessObject_shouldComposeCorrectly() {
        // Arrange
        String container = "cont";
        String source = "src";
        String process = "proc";
        String objectName = "obj";

        // Act
        String name = ObjectStoreUtil.getName(container, source, process, objectName);

        // Assert
        assertEquals("cont/src/proc/obj", name);
    }

    @Test
    void getName_objectTag_shouldComposeCorrectly_andHandleEmpty() {
        // Arrange
        String objectName = "obj";
        String tagName = "tag";

        // Act
        String name = ObjectStoreUtil.getName(objectName, tagName);

        // Assert
        assertEquals("obj/tag", name);

        // when tagName empty
        String onlyObject = ObjectStoreUtil.getName(objectName, "");
        // implementation appends separator when objectName is present
        assertEquals("obj/", onlyObject);

        // when objectName empty
        String onlyTag = ObjectStoreUtil.getName("", tagName);
        assertEquals("tag", onlyTag);
    }
}
