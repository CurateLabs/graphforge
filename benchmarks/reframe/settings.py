site_configuration = {
    "systems": [
        {
            "name": "graphforge-local",
            "descr": "Native local GraphForge benchmark admission",
            "hostnames": [".*"],
            "partitions": [
                {
                    "name": "local",
                    "scheduler": "local",
                    "launcher": "local",
                    "environs": ["builtin"],
                }
            ],
        }
    ]
}
