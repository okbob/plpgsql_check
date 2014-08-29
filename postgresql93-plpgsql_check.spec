%global pgmajorversion 93
%global pginstdir /usr/pgsql-9.3
%global sname plpgsql_check

Name:		%{sname}_%{pgmajorversion}
Version:	0.9.2
Release:	1%{?dist}
Summary:	Additional tools for plpgsql functions validation

Group:		Applications/Databases
License:	BSD
URL:		https://github.com/okbob/plpgsql_check
Source0:	https://github.com/okbob/plpgsql_check/archive/plpgsql_check-0.9.tgz
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	postgresql%{pgmajorversion}-devel
Requires:	postgresql%{pgmajorversion}

%description
The plpgsql_check is PostgreSQL extension with functionality for direct
or indirect extra validation of functions in plpgsql language. It verifies
a validity of SQL identifiers used in plpgsql code. It try to identify
a performance issues.

%prep
%setup -q -n %{sname}


%build
PATH="%{pginstdir}/bin;$PATH" ; export PATH
CFLAGS="${CFLAGS:-%optflags}" ; export CFLAGS

USE_PGXS=1 make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make USE_PGXS=1 DESTDIR=%{buildroot} install

%clean
rm -rf %{buildroot}

%files
%defattr(644,root,root,755)
%doc README.md
%{pginstdir}/lib/plpgsql_check.so
%{pginstdir}/share/extension/plpgsql_check--0.9.sql
%{pginstdir}/share/extension/plpgsql_check.control


%changelog
* Mon Aug 25 2014 - Pavel STEHULE <pavel.stehule@gmail.com> 0.9.1-1
- Initial packaging
