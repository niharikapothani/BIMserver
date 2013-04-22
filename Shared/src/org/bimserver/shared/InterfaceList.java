package org.bimserver.shared;

/******************************************************************************
 * Copyright (C) 2009-2013  BIMserver.org
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.bimserver.interfaces.SServiceInterfaceService;
import org.bimserver.shared.interfaces.AdminInterface;
import org.bimserver.shared.interfaces.AuthInterface;
import org.bimserver.shared.interfaces.Bimsie1Interface;
import org.bimserver.shared.interfaces.LowLevelInterface;
import org.bimserver.shared.interfaces.MetaInterface;
import org.bimserver.shared.interfaces.NotificationInterface;
import org.bimserver.shared.interfaces.PluginInterface;
import org.bimserver.shared.interfaces.PublicInterface;
import org.bimserver.shared.interfaces.RegistryInterface;
import org.bimserver.shared.interfaces.RemoteServiceInterface;
import org.bimserver.shared.interfaces.ServiceInterface;
import org.bimserver.shared.interfaces.SettingsInterface;
import org.bimserver.shared.meta.SService;
import org.bimserver.shared.meta.SServicesMap;
import org.bimserver.shared.meta.SourceCodeFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterfaceList {
	private static final class CodeFetcher implements SourceCodeFetcher {
		@Override
		public String get(Class<?> clazz) {
			URL url = clazz.getResource(clazz.getSimpleName() + ".java");
			if (url == null) {
				try {
					url = new File("../Shared/src/org/bimserver/shared/interfaces/" + clazz.getSimpleName() + ".java").toURI().toURL();
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
			}
			if (url != null) {
				try {
					InputStream inputStream = url.openStream();
					if (inputStream == null) {
						return null;
					}
					StringWriter out = new StringWriter();
					IOUtils.copy(inputStream, out);
					return out.toString();
				} catch (IOException e) {
					LOGGER.error("", e);
				}
			}
			return null;
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(InterfaceList.class);
	private static final Set<Class<? extends PublicInterface>> interfaces = new LinkedHashSet<Class<? extends PublicInterface>>();
	
	static {
		interfaces.add(ServiceInterface.class);
		interfaces.add(NotificationInterface.class);
		interfaces.add(RemoteServiceInterface.class);
		interfaces.add(AdminInterface.class);
		interfaces.add(MetaInterface.class);
		interfaces.add(SettingsInterface.class);
		interfaces.add(AuthInterface.class);
		interfaces.add(LowLevelInterface.class);
		interfaces.add(PluginInterface.class);
		interfaces.add(RegistryInterface.class);
		interfaces.add(Bimsie1Interface.class);
	}

	public static SServicesMap createSServicesMap() {
		SServicesMap servicesMap = new SServicesMap();
		CodeFetcher sourceCodeFetcher = new CodeFetcher();
		SService serviceInterface = new SServiceInterfaceService(sourceCodeFetcher, ServiceInterface.class);
		servicesMap.add(serviceInterface);
		List<SService> singletonList = Collections.singletonList(serviceInterface);
		for (Class<? extends PublicInterface> clazz : getInterfaces()) {
			if (clazz != ServiceInterface.class) {
				servicesMap.add(new SService(sourceCodeFetcher, clazz, singletonList));
			}
		}
		return servicesMap;
	}

	public static Set<Class<? extends PublicInterface>> getInterfaces() {
		return interfaces;
	}
}