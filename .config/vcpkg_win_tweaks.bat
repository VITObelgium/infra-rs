mise exec -- fd --base-directory target/vcpkg/installed -g gdal.pc --exec mise exec -- sd -F -- "-l-framework" "framework"
mise exec -- fd --base-directory target/vcpkg/installed -g gdal.pc --exec mise exec -- sd -F -- " shlwapi" " -lshlwapi"
