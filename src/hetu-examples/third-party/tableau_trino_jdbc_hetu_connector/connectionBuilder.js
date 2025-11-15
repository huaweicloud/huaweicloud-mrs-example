(function dsbuilder(attr) {
    var urlBuilder = "jdbc:trino://" + attr[connectionHelper.attributeServer] + "?serviceDiscoveryMode=" + attr["v-mode"] + "&tenant=" + attr["v-tenant"];

    return [urlBuilder];
})

