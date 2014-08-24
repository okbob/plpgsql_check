%global pgmajorversion 9
%global sname plpgsql_check

%{!?runselftest:%global runselftest 1}

Summary:        allow to check any plpgsql function against database dictionary
Name:           %{sname}%{pgmajorversion}
Version:        0.9
Release:        1%{?dist}
License:        BSD
Group:          Applications/Databases
Source0:        https://github.com/okbob/plpgsql_check/archive/plpgsql_check-0.9.tgz

URL:            https://github.com/okbob/plpgsql_check
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
	
BuildRequires:  postgresql%{pgmajorversion}-devel
Requires:       postgresql%{pgmajorversion}
	
%description
I founded this project, because I wanted to publish the code I wrote in the last two
years, when I tried to write enhanced checking for PostgreSQL upstream. It was not
fully successful - integration into upstream requires some larger plpgsql refactoring
- probably it will not be done in next two years (now is Dec 2013). But written code
is fully functional and can be used in production. So, I created this extension to be
available for all plpgsql developers.
%prep
%setup -q -n %{sname}-%{version}
	
%build
CFLAGS="${CFLAGS:-%optflags}" ; export CFLAGS

USE_PGXS=1 make %{?_smp_mflags}
	
%install
rm -rf $RPM_BUILD_ROOT
mkdir  -p $RPM_BUILD_ROOT/usr/share/doc
make USE_PGXS=1 install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%if %runselftest
	make USE_PGXS=1 installcheck
%endif


%files
%defattr(-,root,root,-)
%doc README.md
%{_datadir}/pgsql/extension/
%{_datadir}/doc/
%{_libdir}/pgsql/plpgsql_check.so
